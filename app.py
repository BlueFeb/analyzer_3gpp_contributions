"""
3GPP Contribution Analyzer v2 — Lightweight Edition
====================================================
Output 1: Conclusions 취합 .docx (원본과 동일)
Output 2: TF-IDF Proposal Summary .docx (원본과 동일)
Output 3: Gemini 의미 분석 (선택, 서버 키 고정)

Cloud Function으로 다운로드/파싱 위임 가능 (미설정 시 직접 처리)
"""

import streamlit as st
import os
import tempfile
import zipfile
import requests
import numpy as np
import re
import io
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from openpyxl import load_workbook
from docx import Document
from docx.table import Table
from docx.text.paragraph import Paragraph
from sklearn.cluster import AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import google.generativeai as genai

# ==========================================
# 1. Page Config & Session State
# ==========================================
st.set_page_config(page_title="3GPP Analyzer v2", page_icon="📡", layout="wide")

DEFAULTS = {
    "authenticated": False,
    "process_done": False,
    "extracted_data": [],
    "out1_bytes": None,
    "out2_bytes": None,
    "notebooklm_txt": None,
    "log_text": "",
    "ai_summary_generated": False,
    "ai_summary_bytes": None,
    "ai_summary_text": "",
    "ai_model_name": "",
    # Meeting/agenda selection
    "meeting_list": [],
    "agenda_dict": {},
    "all_entries": [],
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


def append_log(text):
    st.session_state.log_text += f"{text}\n"


# ==========================================
# 2. Config
# ==========================================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "") or st.secrets.get("GEMINI_API_KEY", "")
CLOUD_FUNCTION_URL = os.environ.get("CLOUD_FUNCTION_URL", "") or st.secrets.get("CLOUD_FUNCTION_URL", "")


# ==========================================
# 2b. 회사명 정규화 (동일 그룹 통합)
# ==========================================
COMPANY_ALIASES = {
    # ZTE 그룹
    "sanechips": "ZTE",
    "zte corporation": "ZTE",
    "zte wistron": "ZTE",
    "zte": "ZTE",
    # Huawei 그룹
    "hisilicon": "Huawei",
    "hisillicon": "Huawei",
    "huawei technologies": "Huawei",
    "huawei": "Huawei",
    "huawei, hisilicon": "Huawei",
    "hisilicon, huawei": "Huawei",
    # 기타 흔한 변형
    "samsung electronics": "Samsung",
    "samsung": "Samsung",
    "qualcomm incorporated": "Qualcomm",
    "qualcomm inc.": "Qualcomm",
    "qualcomm": "Qualcomm",
    "nokia corporation": "Nokia",
    "nokia, nokia shanghai bell": "Nokia",
    "nokia shanghai bell": "Nokia",
    "lg electronics": "LG Electronics",
    "apple inc.": "Apple",
    "ericsson": "Ericsson",
    "mediatek inc.": "MediaTek",
    "mediatek": "MediaTek",
    "oppo": "OPPO",
    "vivo": "vivo",
    "xiaomi": "Xiaomi",
    "catt": "CATT",
    "china telecom": "China Telecom",
    "china mobile": "China Mobile",
    "china unicom": "China Unicom",
    "intel corporation": "Intel",
    "intel": "Intel",
    "interdigital": "InterDigital",
}


def normalize_company(name):
    """회사명을 정규화. 'Sanechips' → 'ZTE', 'HiSilicon' → 'Huawei' 등."""
    if not name or not name.strip():
        return name or ""
    cleaned = name.strip()
    lower = cleaned.lower()
    # 정확 매칭
    if lower in COMPANY_ALIASES:
        return COMPANY_ALIASES[lower]
    # 부분 매칭: alias 키가 회사명에 포함되는 경우만 (최소 3글자 이상)
    for alias_key, alias_val in COMPANY_ALIASES.items():
        if len(alias_key) >= 3 and alias_key in lower:
            return alias_val
    # 매칭 안 되면 원본 반환
    return cleaned


def _safe_filename(text, max_len=40):
    """파일명에 사용할 수 없는 문자 제거 및 길이 제한."""
    if not text:
        return "unknown"
    safe = re.sub(r'[\\/:*?"<>|]', '_', str(text))
    safe = re.sub(r'\s+', '_', safe)
    safe = safe.strip('_')
    return safe[:max_len] if safe else "unknown"


# ==========================================
# 3. 원본 그대로 — 유틸리티 함수들
# ==========================================
def read_excel_from_bytes(uploaded_file):
    wb = load_workbook(uploaded_file, read_only=False, data_only=True)
    ws = wb.active
    entries = []
    for row in ws.iter_rows(min_row=2):
        cell = row[0]
        comp = row[2] if len(row) > 2 else None
        docid = str(cell.value).strip() if cell.value else ""
        company = normalize_company(str(comp.value).strip()) if comp and comp.value else ""
        if not docid:
            continue
        if getattr(cell, "hyperlink", None) and cell.hyperlink.target:
            link = cell.hyperlink.target
        else:
            link = f"https://www.3gpp.org/ftp/tsg_ran/WG1_RL1/TSGR1_122/Docs/{docid}.zip"
        entries.append({"doc": docid, "company": company, "link": link})
    return entries


# ==========================================
# 3b. 회의 번호 → FTP에서 TDoc 리스트 xlsx 자동 조회
# ==========================================
# WG별 FTP 경로 매핑
WG_FTP_MAP = {
    "RAN1": "tsg_ran/WG1_RL1",
    "RAN2": "tsg_ran/WG2_RL2",
    "RAN3": "tsg_ran/WG3_Iu",
    "RAN4": "tsg_ran/WG4_Radio",
    "SA1": "tsg_sa/WG1_Serv",
    "SA2": "tsg_sa/WG2_Arch",
    "SA3": "tsg_sa/WG3_Security",
    "SA4": "tsg_sa/WG4_CODEC",
    "SA5": "tsg_sa/WG5_TM",
    "SA6": "tsg_sa/WG6_MissionCritical",
    "CT1": "tsg_ct/WG1_mm-cc-sm_ex-CN1",
    "CT3": "tsg_ct/WG3_interworking_ex-CN3",
    "CT4": "tsg_ct/WG4_protocollars_ex-CN4",
}

# WG별 회의 폴더 prefix (FTP에서 실제 확인된 값)
WG_MEETING_PREFIXES = {
    "RAN1": ["TSGR1_"],
    "RAN2": ["TSGR2_"],
    "RAN3": ["TSGR3_"],
    "RAN4": ["TSGR4_"],
    "SA1":  ["TSGS1_"],
    "SA2":  ["TSGS2_"],
    "SA3":  ["TSGS3_"],
    "SA4":  ["TSGS4_"],
    "SA5":  ["TSGS5_"],
    "SA6":  ["TSGS6_"],
    "CT1":  ["TSGC1_"],
    "CT3":  ["TSGC3_"],
    "CT4":  ["CT4_"],
}

# WG별 TDoc 리스트 xlsx 파일명 패턴
WG_TDOC_PREFIX = {
    "RAN1": "TDoc_List_Meeting_RAN1#",
    "RAN2": "TDoc_List_Meeting_RAN2#",
    "RAN3": "TDoc_List_Meeting_RAN3#",
    "RAN4": "TDoc_List_Meeting_RAN4#",
    "SA1": "TDoc_List_Meeting_SA1#",
    "SA2": "TDoc_List_Meeting_SA2#",
    "SA3": "TDoc_List_Meeting_SA3#",
    "SA4": "TDoc_List_Meeting_SA4#",
    "SA5": "TDoc_List_Meeting_SA5#",
    "SA6": "TDoc_List_Meeting_SA6#",
    "CT1": "TDoc_List_Meeting_CT1#",
    "CT3": "TDoc_List_Meeting_CT3#",
    "CT4": "TDoc_List_Meeting_CT4#",
}


def _request_with_retry(url, method="get", max_retries=3, timeout=60, **kwargs):
    """3GPP FTP 서버 요청에 재시도 로직 추가. 서버가 느리거나 불안정할 때 대응."""
    kwargs.setdefault("verify", False)
    kwargs.setdefault("headers", {"User-Agent": "Mozilla/5.0"})
    kwargs["timeout"] = timeout

    last_error = None
    for attempt in range(max_retries):
        try:
            if method == "head":
                r = requests.head(url, **kwargs)
            else:
                r = requests.get(url, **kwargs)
            if r.status_code == 200:
                return r
            last_error = f"HTTP {r.status_code}"
        except requests.exceptions.Timeout:
            last_error = f"Timeout ({timeout}초)"
            append_log(f"3GPP 서버 타임아웃 (시도 {attempt+1}/{max_retries}): {url[:80]}")
        except requests.exceptions.ConnectionError:
            last_error = "연결 실패"
            append_log(f"3GPP 서버 연결 실패 (시도 {attempt+1}/{max_retries}): {url[:80]}")
        except Exception as e:
            last_error = str(e)

        if attempt < max_retries - 1:
            time.sleep(3 * (attempt + 1))  # 3초, 6초, 9초 대기

    append_log(f"3GPP 서버 요청 최종 실패: {last_error}")
    return None


def list_meetings_from_ftp(wg):
    """FTP 디렉토리 목록에서 해당 WG의 회의 폴더 목록을 가져온다."""
    ftp_path = WG_FTP_MAP.get(wg)
    if not ftp_path:
        return []
    url = f"https://www.3gpp.org/ftp/{ftp_path}/"
    try:
        r = requests.get(url, timeout=15, verify=False)
        r.raise_for_status()
        # HTML 디렉토리 리스팅에서 폴더명 추출
        # href="TSGR2_133bis/" 또는 href="/ftp/.../TSGR2_133bis/" 패턴
        all_links = re.findall(r'href="([^"]*)"', r.text)
        prefixes = WG_MEETING_PREFIXES.get(wg, [])
        meetings = []
        seen = set()
        for link in all_links:
            name = link.rstrip("/").split("/")[-1]
            if not name or name in seen:
                continue
            # 여러 prefix 후보 중 하나라도 매칭되면 회의 폴더
            for pfx in prefixes:
                if name.upper().startswith(pfx.upper()):
                    meetings.append(name)
                    seen.add(name)
                    break
        # 숫자 기준 최신순 정렬
        def sort_key(m):
            nums = re.findall(r'\d+', m)
            return int(nums[0]) if nums else 0
        meetings.sort(key=sort_key, reverse=True)
        return meetings[:30]  # 최근 30개
    except Exception as e:
        append_log(f"FTP 회의 목록 조회 오류: {e}")
        return []


def resolve_meeting_folder(wg, meeting_num):
    """
    사용자가 입력한 회의 번호(예: 168, 131bis)로 FTP에서 실제 폴더명을 찾는다.
    RAN 그룹: TSGR2_133bis (번호+bis 그대로)
    RAN3: TSGR3_131-bis (하이픈 변형도 있음)
    SA/CT 그룹: TSGS2_168_Goteborg_2025-04 (도시명+날짜 포함)

    반환: 실제 폴더명 문자열, 못 찾으면 None
    """
    ftp_path = WG_FTP_MAP.get(wg, "")
    prefixes = WG_MEETING_PREFIXES.get(wg, [])
    if not prefixes:
        return None

    # 후보 폴더명 여러 변형 생성
    # 사용자 입력: 124bis, 131bis, 133bis 등
    # 실제 폴더: TSGR1_124b, TSGR3_131-bis, TSGR2_133bis 등
    base = f"{prefixes[0]}{meeting_num}"
    candidates_to_try = [base]

    num_match = re.match(r'(\d+)', meeting_num)
    if num_match:
        num_part = num_match.group(1)
        suffix = meeting_num[len(num_part):].lower().lstrip("-_")  # "bis", "e", "b" 등

        if suffix:
            # 모든 변형 추가: bis, -bis, _bis, b, -b, BIS
            all_suffixes = set()
            all_suffixes.add(suffix)                    # bis
            all_suffixes.add(f"-{suffix}")              # -bis
            all_suffixes.add(f"_{suffix}")              # _bis
            if suffix == "bis":
                all_suffixes.add("b")                   # b (RAN1 스타일)
                all_suffixes.add("-b")
            elif suffix == "b":
                all_suffixes.add("bis")                 # bis
                all_suffixes.add("-bis")                # -bis

            for sfx in all_suffixes:
                candidate = f"{prefixes[0]}{num_part}{sfx}"
                if candidate not in candidates_to_try:
                    candidates_to_try.append(candidate)

    # 시도 1: 단순 이름 변형들로 바로 접근
    for candidate in candidates_to_try:
        test_url = f"https://www.3gpp.org/ftp/{ftp_path}/{candidate}/Docs/"
        r = _request_with_retry(test_url, method="head", max_retries=2, timeout=15)
        if r and r.status_code == 200:
            return candidate

    # 시도 2: FTP 디렉토리 리스팅에서 매칭되는 폴더 찾기
    dir_url = f"https://www.3gpp.org/ftp/{ftp_path}/"
    r = _request_with_retry(dir_url, max_retries=3, timeout=30)
    if not r:
        return None
    try:

        all_links = re.findall(r'href="([^"?]+)"', r.text)
        all_links += re.findall(r'>([A-Z][^<]{3,80})<', r.text)

        # 숫자 부분만 추출해서 매칭 (131bis, 131-bis, 131_bis 모두 → "131" + "bis")
        num_match = re.match(r'(\d+)', meeting_num)
        if not num_match:
            return None
        num_part = num_match.group(1)
        suffix_part = meeting_num[len(num_part):].lower()  # "bis", "e", "" 등

        found = []
        for link in all_links:
            name = link.rstrip("/").split("/")[-1].strip()
            if not name:
                continue
            name_upper = name.upper()
            prefix_upper = prefixes[0].upper()
            if not name_upper.startswith(prefix_upper):
                continue
            # prefix 뒤의 부분에서 숫자 추출
            after_prefix = name[len(prefixes[0]):]
            folder_num_match = re.match(r'(\d+)', after_prefix)
            if not folder_num_match:
                continue
            folder_num = folder_num_match.group(1)
            if folder_num != num_part:
                continue
            # 숫자 뒤 부분 체크
            folder_rest = after_prefix[len(folder_num):].lower().lstrip("-_")
            if suffix_part:
                # suffix 정규화: b ↔ bis 동일 취급
                normalized_suffix = suffix_part.replace("-", "").replace("_", "")
                normalized_folder = folder_rest.replace("-", "").replace("_", "")
                # "b" 와 "bis" 를 동일하게 취급
                def normalize_bis(s):
                    if s.startswith("bis"): return "bis" + s[3:]
                    if s.startswith("b") and (len(s) == 1 or not s[1].isalpha()): return "bis" + s[1:]
                    return s
                if normalize_bis(normalized_folder).startswith(normalize_bis(normalized_suffix)):
                    found.append(name)
            else:
                # suffix 없으면 (순수 숫자 입력, 예: "156")
                # ✅ 매칭: TSGS2_156 (뒤에 아무것도 없음)
                # ✅ 매칭: TSGS2_156_Toulouse (뒤에 _도시명)
                # ✅ 매칭: TSGS2_156E_Electronic (뒤에 E=electronic)
                # ✅ 매칭: TSGS2_156AH_xxx (뒤에 AH=ad hoc)
                # ❌ 불매칭: TSGS2_156b_xxx (b는 별도 회의 번호)
                # ❌ 불매칭: TSGS2_156c_xxx (c도 별도 회의 번호)
                # ❌ 불매칭: TSGS2_156bis_xxx (bis도 별도)
                if (folder_rest == "" or
                    folder_rest.startswith("_") or
                    folder_rest.startswith("/") or
                    re.match(r'^(e|ah|ahe?)[\W_]', folder_rest, re.I) or
                    re.match(r'^(e|ah|ahe?)$', folder_rest, re.I)):
                    found.append(name)

        if found:
            found.sort(key=len)
            append_log(f"폴더 후보: {found}")
            return found[0]

    except Exception as e:
        append_log(f"폴더 검색 오류: {e}")

    return None


def fetch_tdoc_list_xlsx(wg, meeting_folder):
    """
    회의 폴더의 Docs/ 안에서 TDoc_List xlsx를 다운받아 파싱.
    반환: (agenda_dict, entries)
      agenda_dict: {"7.1 - AI/ML": [entry1, entry2, ...], ...}
      entries: 전체 entry 리스트
    """
    import urllib.parse

    ftp_path = WG_FTP_MAP.get(wg, "")
    tdoc_prefix = WG_TDOC_PREFIX.get(wg, "TDoc_List_Meeting_")
    
    # 회의 번호 추출: 여러 prefix 후보에서 매칭되는 것 제거
    meeting_num = meeting_folder
    for pfx in WG_MEETING_PREFIXES.get(wg, []):
        if meeting_folder.upper().startswith(pfx.upper()):
            meeting_num = meeting_folder[len(pfx):]
            break

    # SA2의 경우: TSGS2_168_City → 168, TSGS2_168bis_City → 168bis
    # RAN3의 경우: TSGR3_131-bis → 131-bis (하이픈 보존)
    # 도시명 등 부가 문자 제거: 숫자(+하이픈/bis/e 등)만 추출
    match = re.match(r'^(\d+(?:-?bis|-?e|-?b)?)', meeting_num, re.I)
    if match:
        meeting_num = match.group(1)

    docs_url = f"https://www.3gpp.org/ftp/{ftp_path}/{meeting_folder}/Docs/"

    # TDoc 리스트 xlsx 파일명 구성 — 여러 변형 시도
    # RAN1: 폴더 124b → xlsx는 #124-bis.xlsx
    # RAN3: 폴더 131-bis → xlsx는 #131-bis.xlsx
    # RAN2: 폴더 133bis → xlsx는 #133bis.xlsx
    xlsx_candidates = [f"{tdoc_prefix}{meeting_num}.xlsx"]

    # meeting_num에서 숫자와 suffix 분리
    mn_match = re.match(r'^(\d+)[-_]?(.*)', meeting_num, re.I)
    if mn_match:
        mn_num = mn_match.group(1)
        mn_suffix = mn_match.group(2).lower()  # "bis", "b", "e", "" 등

        # 모든 변형 생성
        suffix_variants = set()
        if mn_suffix:
            suffix_variants.add(mn_suffix)                    # bis, b, e
            suffix_variants.add(f"-{mn_suffix}")              # -bis, -b
            if mn_suffix == "b":
                suffix_variants.add("bis")
                suffix_variants.add("-bis")
            elif mn_suffix == "bis":
                suffix_variants.add("b")
                suffix_variants.add("-b")

            for sfx in suffix_variants:
                candidate = f"{tdoc_prefix}{mn_num}{sfx}.xlsx"
                if candidate not in xlsx_candidates:
                    xlsx_candidates.append(candidate)

    # 시도 1 & 2: 모든 파일명 후보를 순서대로 시도 (재시도 포함)
    r = None
    for xlsx_filename in xlsx_candidates:
        xlsx_url_encoded = f"{docs_url}{urllib.parse.quote(xlsx_filename)}"
        resp = _request_with_retry(xlsx_url_encoded, max_retries=3, timeout=60)
        if resp and resp.status_code == 200:
            r = resp
            append_log(f"TDoc xlsx 발견: {xlsx_filename}")
            break

    # 시도 3: Docs 폴더 HTML을 파싱해서 실제 xlsx 파일명 찾기
    if r is None:
        dir_resp = _request_with_retry(docs_url, max_retries=2, timeout=30)
        if dir_resp:
            try:
                xlsx_links = re.findall(r'href="([^"]*TDoc_List[^"]*\.xlsx)"', dir_resp.text, re.I)
                if xlsx_links:
                    actual_filename = xlsx_links[0].split("/")[-1]
                    actual_url = f"{docs_url}{urllib.parse.quote(actual_filename)}"
                    r = _request_with_retry(actual_url, max_retries=3, timeout=60)
            except Exception as e:
                append_log(f"TDoc 리스트 디렉토리 검색 실패: {e}")

    if r is None:
        append_log(f"TDoc 리스트 다운로드 최종 실패")
        return {}, []

    # Parse xlsx
    wb = load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    ws = wb.active

    # 헤더 행 찾기 — 상위 10행까지 스캔
    header_row = None
    col_map = {}
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=10), start=1):
        for col_idx, cell in enumerate(row):
            val = str(cell.value or "").strip().lower()
            # TDoc 컬럼
            if any(kw in val for kw in ["tdoc", "td#", "td number"]) and "tdoc" not in col_map:
                col_map["tdoc"] = col_idx
            # Source/Company 컬럼
            if any(kw in val for kw in ["source", "company", "submitting"]) and "company" not in col_map:
                col_map["company"] = col_idx
            # Agenda 컬럼 — "agenda item description"을 우선, "agenda item"만 있으면 그것 사용
            if "agenda" in val:
                if "description" in val:
                    # "Agenda item description" → 이게 진짜 agenda 텍스트
                    col_map["agenda"] = col_idx
                elif "agenda" not in col_map:
                    # "Agenda item" (번호) → description이 없을 때만 사용
                    col_map["agenda_num"] = col_idx
        if "tdoc" in col_map and ("agenda" in col_map or "agenda_num" in col_map):
            header_row = row_idx
            break

    # agenda description이 없으면 agenda_num 사용
    if "agenda" not in col_map and "agenda_num" in col_map:
        col_map["agenda"] = col_map["agenda_num"]

    # agenda_num이 별도로 있으면 번호+설명을 합쳐서 사용
    has_separate_num = "agenda_num" in col_map and "agenda" in col_map and col_map.get("agenda_num") != col_map.get("agenda")

    # Fallback: 3GPP 표준 레이아웃 (A=TDoc, C=Source/Company, K=Agenda item, L=Agenda desc)
    if not header_row:
        header_row = 1
        col_map = {"tdoc": 0, "company": 2, "agenda_num": 10, "agenda": 11}
        has_separate_num = True

    entries = []
    agenda_dict = {}

    for row in ws.iter_rows(min_row=header_row + 1):
        tdoc_idx = col_map.get("tdoc", 0)
        company_idx = col_map.get("company", 2)
        agenda_idx = col_map.get("agenda", 11)
        agenda_num_idx = col_map.get("agenda_num", 10)

        if len(row) <= tdoc_idx:
            continue

        tdoc_cell = row[tdoc_idx]
        company_cell = row[company_idx] if len(row) > company_idx else None
        agenda_cell = row[agenda_idx] if len(row) > agenda_idx else None
        agenda_num_cell = row[agenda_num_idx] if has_separate_num and len(row) > agenda_num_idx else None

        tdoc_id = str(tdoc_cell.value or "").strip()
        if not tdoc_id:
            continue

        company = normalize_company(str(company_cell.value or "").strip()) if company_cell else ""
        agenda_desc = str(agenda_cell.value or "").strip() if agenda_cell else ""
        agenda_num = str(agenda_num_cell.value or "").strip() if agenda_num_cell else ""

        # 번호 + 설명 합치기: "9.3.2.5 - Discussion on XYZ"
        if agenda_num and agenda_desc and agenda_num != agenda_desc:
            agenda = f"{agenda_num} - {agenda_desc}"
        elif agenda_desc:
            agenda = agenda_desc
        elif agenda_num:
            agenda = agenda_num
        else:
            agenda = ""

        # 하이퍼링크에서 다운로드 URL 추출
        if getattr(tdoc_cell, "hyperlink", None) and tdoc_cell.hyperlink.target:
            link = tdoc_cell.hyperlink.target
        else:
            link = f"{docs_url}{tdoc_id}.zip"

        entry = {"doc": tdoc_id, "company": company, "link": link, "agenda": agenda}
        entries.append(entry)

        if agenda:
            agenda_dict.setdefault(agenda, [])
            agenda_dict[agenda].append(entry)

    wb.close()
    return agenda_dict, entries


def clone_paragraph(src, dest):
    np_para = dest.add_paragraph("", style=src.style)
    for r in src.runs:
        nr = np_para.add_run(r.text)
        nr.bold = r.bold
        nr.italic = r.italic
        nr.underline = r.underline
        if hasattr(r.font, "name") and r.font.name:
            nr.font.name = r.font.name
        if hasattr(r.font, "size") and r.font.size:
            nr.font.size = r.font.size
        if hasattr(r.font, "color") and getattr(r.font.color, "rgb", None):
            nr.font.color.rgb = r.font.color.rgb
    return np_para


def repackage_docm_to_docx(path, td):
    ud = os.path.join(td, "docm_unzip")
    os.makedirs(ud, exist_ok=True)
    with zipfile.ZipFile(path, 'r') as zf:
        zf.extractall(ud)
    tf = os.path.join(ud, "[Content_Types].xml")
    if not os.path.exists(tf):
        return path  # Content_Types.xml 없으면 원본 반환
    with open(tf, 'r', encoding='utf-8') as f:
        t = f.read()
    t = t.replace(
        'application/vnd.ms-word.document.macroEnabled.main+xml',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml'
    )
    with open(tf, 'w', encoding='utf-8') as f:
        f.write(t)
    rp = os.path.join(td, "repack.zip")
    with zipfile.ZipFile(rp, 'w', zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(ud):
            for f in fs:
                full = os.path.join(r, f)
                arc = os.path.relpath(full, ud)
                zf.write(full, arc)
    out = os.path.join(td, "repack.docx")
    os.rename(rp, out)
    return out


def _download_doc(entry, td_name, headers):
    try:
        kwargs = {"headers": headers, "timeout": 60, "verify": False}
        r = requests.get(entry["link"], **kwargs)
        r.raise_for_status()
        fp = os.path.join(td_name, f"{entry['doc']}.zip")
        with open(fp, "wb") as f:
            f.write(r.content)
        return entry, fp, None
    except Exception as ex:
        return entry, None, str(ex)


# ==========================================
# 4. Output 1 — extract_all_conclusions (원본 동일)
# ==========================================
def _cleanup_tmp_if_low_disk():
    """
    디스크 잔여 용량이 전체의 10% 미만이면 /tmp 안의 이전 다운로드 잔류물을 정리.
    Cloud Function은 자동 정리되지만, Streamlit 서버에서 직접 처리 시
    비정상 종료 등으로 /tmp에 파일이 남을 수 있음.
    """
    import shutil
    try:
        tmp_dir = tempfile.gettempdir()
        disk = shutil.disk_usage(tmp_dir)
        free_pct = disk.free / disk.total * 100

        if free_pct < 10:
            append_log(f"⚠️ 디스크 여유 공간 부족 ({free_pct:.1f}%). /tmp 정리 시작...")
            cleaned = 0
            for item in os.listdir(tmp_dir):
                item_path = os.path.join(tmp_dir, item)
                # 3GPP 관련 임시 파일/폴더만 삭제 (안전)
                if any(kw in item.lower() for kw in ["tmp", "r1-", "r2-", "r3-", "r4-",
                                                       "s1-", "s2-", "s3-", "c1-", "c3-",
                                                       "3gpp", "docm_unzip", "repack"]):
                    try:
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path, ignore_errors=True)
                        else:
                            os.remove(item_path)
                        cleaned += 1
                    except Exception:
                        pass
            append_log(f"정리 완료: {cleaned}개 항목 삭제.")
            try:
                disk2 = shutil.disk_usage(tmp_dir)
                append_log(f"여유 공간: {free_pct:.1f}% → {disk2.free / disk2.total * 100:.1f}%")
            except Exception:
                pass
        else:
            append_log(f"디스크 여유: {free_pct:.1f}% (정리 불필요)")
    except Exception as e:
        append_log(f"디스크 체크 오류 (무시): {e}")


def extract_all_conclusions(entries, status_elem, progress_elem, log_func):
    # 분석 시작 전 디스크 용량 체크 및 정리
    _cleanup_tmp_if_low_disk()

    if CLOUD_FUNCTION_URL:
        return _extract_via_cloud(entries, status_elem, progress_elem, log_func)
    return _extract_local(entries, status_elem, progress_elem, log_func)


def _extract_via_cloud(entries, status_elem, progress_elem, log_func):
    """Cloud Function으로 다운로드/파싱 위임, 결과로 원본과 동일한 docx 생성."""
    od = Document()
    od.add_heading("3GPP Conclusions", level=0)
    extracted_list = []
    total = len(entries)
    if total == 0:
        log_func("입력 문서 없음")
        bio = io.BytesIO()
        od.save(bio)
        bio.seek(0)
        return bio
    batch_size = 10
    all_results = []

    for i in range(0, total, batch_size):
        batch = entries[i:i + batch_size]
        status_elem.text(f"☁️ 클라우드 처리 [{min(i+batch_size, total)}/{total}]")
        progress_elem.progress(min(i+batch_size, total) / max(total, 1))
        try:
            resp = requests.post(CLOUD_FUNCTION_URL, json={"entries": batch}, timeout=300)
            resp.raise_for_status()
            all_results.extend(resp.json().get("results", []))
        except Exception as e:
            log_func(f"Cloud Function 오류, 로컬 전환: {e}")
            return _extract_local(entries, status_elem, progress_elem, log_func)

    for idx, item in enumerate(all_results, 1):
        tbl = od.add_table(rows=4, cols=2, style="Table Grid")
        tbl.cell(0,0).text, tbl.cell(0,1).text = "Document", item.get("doc","")
        tbl.cell(1,0).text, tbl.cell(1,1).text = "Link", item.get("link","")
        tbl.cell(2,0).text, tbl.cell(2,1).text = "Company", item.get("company","")
        tbl.cell(3,0).text, tbl.cell(3,1).text = "Title", item.get("title","")

        content = item.get("content","")
        if content and content not in ("결론 섹션 없음","DOC 파일 없음"):
            for line in content.split("\n"):
                if line.strip():
                    od.add_paragraph(line)
        else:
            od.add_paragraph(content or "결론 섹션 없음")

        extracted_list.append({
            "doc": item.get("doc",""), "company": item.get("company",""),
            "link": item.get("link",""), "title": item.get("title",""),
            "content": content,
            "full_content": content,
        })
        log_func(f"{item.get('doc','')} 추출 완료")
        if idx < len(all_results):
            od.add_page_break()

    st.session_state.extracted_data = extracted_list
    _build_notebooklm_txt(extracted_list)
    bio = io.BytesIO()
    od.save(bio)
    bio.seek(0)
    return bio


def _extract_local(entries, status_elem, progress_elem, log_func):
    """원본 extract_all_conclusions과 동일."""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_func(f"임시 디렉터리 생성: {temp_dir}")
        od = Document()
        od.add_heading("3GPP Conclusions", level=0)

        cps = [
            # "3. Conclusion", "Conclusions", "3 Conclusions"
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(conclusions?)\s*$", re.I),
            # "Conclusion and proposals", "Conclusions and Observations"
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(conclusions?\s+and\s+\w+)", re.I),
            # "Summary", "3. Summary"
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(summary)\s*$", re.I),
            # "Summary and proposal", "Summary and observations"
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(summary\s+and\s+\w+)", re.I),
            # "xxx summary" — e.g. "SIB design summary", "Overall summary"
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(\w+\s+)?summary\s*$", re.I),
            # "Proposals" (단독 섹션)
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(proposals?)\s*$", re.I),
        ]
        eps = [
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(references?|appendix|acknowledgment|annex)\s*", re.I),
        ]
        headers = {"User-Agent": "Mozilla/5.0"}
        download_results = []
        extracted_list = []
        total = len(entries)

        if total == 0:
            log_func("입력 문서 없음")
            bio = io.BytesIO()
            od.save(bio)
            bio.seek(0)
            return bio

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_download_doc, e, temp_dir, headers): e for e in entries}
            for i, fut in enumerate(as_completed(futures), start=1):
                e, fp, err = fut.result()
                download_results.append((e, fp, err))
                progress_elem.progress(i / max(total, 1))
                status_elem.text(f"Downloaded [{i}/{total}]: {e['doc']}")
                log_func(f"[{i}/{total}] Downloaded: {e['doc']}")

        for idx, (e, fp, err) in enumerate(download_results, start=1):
            status_elem.text(f"Extracting [{idx}/{total}]: {e['doc']}")
            doc_text_buffer = []
            full_text_buffer = []

            tbl = od.add_table(rows=4, cols=2, style="Table Grid")
            tbl.cell(0,0).text, tbl.cell(0,1).text = "Document", e["doc"]
            tbl.cell(1,0).text, tbl.cell(1,1).text = "Link", e["link"]
            tbl.cell(2,0).text, tbl.cell(2,1).text = "Company", e["company"]
            tbl.cell(3,0).text = "Title"

            try:
                if err or not fp:
                    raise Exception(err or "Download failed")
                ed = os.path.join(temp_dir, e["doc"])
                os.makedirs(ed, exist_ok=True)
                with zipfile.ZipFile(fp) as zf:
                    zf.extractall(ed)

                src_path = None
                for ext in ("*.docx", "*.docm", "*.doc"):
                    src_path = next(Path(ed).rglob(ext), None)
                    if src_path: break

                # PDF fallback
                if not src_path:
                    src_path = next(Path(ed).rglob("*.pdf"), None)

                if not src_path:
                    od.add_paragraph("DOC/PDF 파일을 찾을 수 없습니다.")
                    log_func(f"{e['doc']} 없음")
                    continue

                file_path_str = str(src_path)

                # .doc (구형 바이너리) 파일 처리 — python-docx로 열 수 없음
                if src_path.suffix.lower() == ".doc":
                    try:
                        # 바이너리에서 텍스트 추출 시도 (완벽하지 않지만 Proposal/Conclusion 키워드 포착 가능)
                        with open(file_path_str, "rb") as bf:
                            raw = bf.read()
                        # .doc에서 ASCII 텍스트 추출
                        text_chunks = re.findall(rb'[\x20-\x7E]{20,}', raw)
                        raw_text = "\n".join(chunk.decode('ascii', errors='ignore') for chunk in text_chunks)
                        if raw_text:
                            od.add_paragraph(f"[구형 .doc — 텍스트 추출 (서식 없음)]")
                            # Conclusion/Summary 부분 찾기
                            lines = raw_text.split('\n')
                            found_conclusion = False
                            for li, line in enumerate(lines):
                                if re.search(r'(?:conclusion|summary)', line, re.I):
                                    found_conclusion = True
                                    for cl in lines[li:li+30]:
                                        od.add_paragraph(cl)
                                        doc_text_buffer.append(cl)
                                    break
                            if not found_conclusion:
                                od.add_paragraph("결론 섹션 없음 (구형 .doc)")
                                # 전체 텍스트 일부 추출
                                for cl in lines[-20:]:
                                    doc_text_buffer.append(cl)
                            extracted_list.append({
                                "doc": e["doc"], "company": e["company"], "link": e["link"],
                                "title": "(구형 .doc)",
                                "content": "\n".join(doc_text_buffer) if doc_text_buffer else "텍스트 추출 실패",
                                "full_content": raw_text[:5000]
                            })
                            log_func(f"{e['doc']} .doc 텍스트 추출")
                        else:
                            od.add_paragraph("구형 .doc 파일에서 텍스트를 추출할 수 없습니다.")
                            log_func(f"{e['doc']} .doc 텍스트 추출 실패")
                    except Exception as ex:
                        od.add_paragraph(f"구형 .doc 파일 처리 오류: {ex}")
                        log_func(f"{e['doc']} .doc 오류: {ex}")
                    if idx < len(download_results):
                        od.add_page_break()
                    continue

                if src_path.suffix.lower() == ".docm":
                    try:
                        file_path_str = repackage_docm_to_docx(file_path_str, temp_dir)
                    except Exception as ex:
                        log_func(f"{e['doc']} docm 변환 오류: {ex}")

                try:
                    sd = Document(file_path_str)
                except Exception as ex:
                    od.add_paragraph(f"문서를 열 수 없습니다 (구형 .doc 파일이거나 손상됨): {ex}")
                    log_func(f"{e['doc']} 문서 파싱 에러: {ex}")
                    continue

                title = ""
                paras = sd.paragraphs
                for p in paras:
                    t = p.text.strip()
                    if t:
                        full_text_buffer.append(t)
                    if not title and t.lower().startswith("title:"):
                        title = t.split(":", 1)[1].strip()
                if not title:
                    title = sd.core_properties.title or ""

                # ★ CR (Change Request) 감지 ★
                # CR 문서는 첫 번째 테이블에 "CHANGE REQUEST" 또는 "CR-Form" 텍스트가 있음
                is_cr = False
                cr_reason = ""
                cr_summary = ""
                cr_title = ""
                try:
                    for tbl_idx, doc_tbl in enumerate(sd.tables[:3]):  # 처음 3개 테이블만 체크
                        for row in doc_tbl.rows:
                            row_text = " ".join(cell.text.strip() for cell in row.cells).lower()
                            if "change request" in row_text or "cr-form" in row_text:
                                is_cr = True
                                break
                        if is_cr:
                            break

                    if is_cr:
                        # CR 테이블에서 Title, Reason for change, Summary of change 추출
                        for doc_tbl in sd.tables[:3]:
                            for row in doc_tbl.rows:
                                cells = [cell.text.strip() for cell in row.cells]
                                cells_lower = [c.lower() for c in cells]
                                row_joined = " ".join(cells_lower)

                                # Title 추출 (보통 Table 2, Row 1)
                                if "title:" in cells_lower[0] and not cr_title:
                                    # 두 번째 셀 이후에 제목이 있음
                                    for c in cells[1:]:
                                        if c and c != cells[0]:
                                            cr_title = c
                                            break

                                # Reason for change
                                if "reason for change" in row_joined:
                                    for c in cells:
                                        if c.lower() not in ("", "reason for change:", "reason for change"):
                                            cr_reason = c
                                            break

                                # Summary of change
                                if "summary of change" in row_joined:
                                    for c in cells:
                                        if c.lower() not in ("", "summary of change:", "summary of change"):
                                            cr_summary = c
                                            break

                        if cr_title and not title:
                            title = cr_title
                except Exception:
                    pass  # 테이블 파싱 실패해도 일반 문서로 계속 진행

                tbl.cell(3, 1).text = title

                # CR 문서 처리
                if is_cr:
                    od.add_paragraph("📋 [CR — Change Request 문서]").runs[0].bold = True
                    if cr_reason:
                        p_label = od.add_paragraph("")
                        p_label.add_run("Reason for change: ").bold = True
                        od.add_paragraph(cr_reason)
                        doc_text_buffer.append(f"Reason for change: {cr_reason}")
                    if cr_summary:
                        p_label = od.add_paragraph("")
                        p_label.add_run("Summary of change: ").bold = True
                        od.add_paragraph(cr_summary)
                        doc_text_buffer.append(f"Summary of change: {cr_summary}")
                    if not cr_reason and not cr_summary:
                        od.add_paragraph("CR 테이블에서 Reason/Summary를 추출하지 못했습니다.")
                    log_func(f"{e['doc']} CR 문서 추출 완료")

                    extracted_list.append({
                        "doc": e["doc"], "company": e["company"], "link": e["link"],
                        "title": title, "is_cr": True,
                        "content": "\n".join(doc_text_buffer) if doc_text_buffer else "CR 내용 추출 실패",
                        "full_content": "\n".join(full_text_buffer) if full_text_buffer else ""
                    })
                    if idx < len(download_results):
                        od.add_page_break()
                    continue

                # ★ 일반 문서 — Conclusion/Summary 섹션 검색 ★
                start = None
                for pat in cps:
                    for j, p in enumerate(paras):
                        if pat.match(p.text.strip()):
                            start = j; break
                    if start is not None: break

                if start is None:
                    od.add_paragraph("결론 섹션 없음")
                    log_func(f"{e['doc']} 결론없음")
                else:
                    end = len(paras)
                    for ep in eps:
                        for j, p in enumerate(paras[start+1:], start+1):
                            if ep.match(p.text.strip()):
                                end = j; break
                        if end < len(paras): break
                    for j in range(start+1, end):
                        clone_paragraph(paras[j], od)
                        doc_text_buffer.append(paras[j].text)
                    log_func(f"{e['doc']} 추출 완료")

                extracted_list.append({
                    "doc": e["doc"], "company": e["company"], "link": e["link"],
                    "title": title,
                    "content": "\n".join(doc_text_buffer) if doc_text_buffer else "Conclusion 섹션을 찾지 못했습니다.",
                    "full_content": "\n".join(full_text_buffer) if full_text_buffer else "원문 텍스트를 추출하지 못했습니다."
                })
            except Exception as ex:
                od.add_paragraph(f"오류 - {e['doc']}: {ex}")
                log_func(str(ex))

            if idx < len(download_results):
                od.add_page_break()

        st.session_state.extracted_data = extracted_list
        _build_notebooklm_txt(extracted_list)
        bio = io.BytesIO()
        od.save(bio)
        bio.seek(0)
    return bio


def _build_notebooklm_txt(extracted_list):
    txt = ["=== 3GPP Contributions Conclusions ==="]
    for item in extracted_list:
        txt.append(f"\n\n--- Document: {item['doc']} ---")
        txt.append(f"Company: {item['company']}")
        txt.append(f"Title: {item['title']}")
        txt.append("Content:")
        txt.append(item['content'])
    st.session_state.notebooklm_txt = "\n".join(txt)


# ==========================================
# 5. Output 2 — TF-IDF parse_and_summarize (원본 동일)
# ==========================================
class TFIDFEmbedder:
    def __init__(self, max_features=3000, ngram_range=(1, 2)):
        self.v = TfidfVectorizer(
            max_features=max_features, ngram_range=ngram_range,
            lowercase=True, stop_words="english", strip_accents="unicode",
            token_pattern=r"\b[a-zA-Z]{2,}\b",
        )
        self.fitted = False

    def encode(self, texts):
        if isinstance(texts, str): texts = [texts]
        proc = [re.sub(r"\s+", " ", re.sub(r"[^\w\s\-]", " ", t.lower())).strip() for t in texts]
        if not self.fitted:
            self.v.fit(proc)
            self.fitted = True
        return self.v.transform(proc).toarray()


def parse_and_summarize(in_bio, status_elem, log_func):
    """원본 parse_and_summarize와 동일."""
    d = Document(in_bio)
    props, pcs, cur = [], {}, None

    for el in d.element.body:
        if el.tag.endswith("tbl"):
            tbl = Table(el, d)
            for r in tbl.rows:
                if r.cells[0].text.strip() == "Company":
                    cur = r.cells[1].text.strip()
        elif el.tag.endswith("p"):
            p = Paragraph(el, d)
            txt = p.text.strip()
            if txt.lower().startswith("proposal"):
                buf, cm = [txt], {cur} if cur else set()
                idx2 = d.element.body.index(el) + 1
                while idx2 < len(d.element.body):
                    sib = d.element.body[idx2]
                    if not sib.tag.endswith("p"): break
                    sp = Paragraph(sib, d)
                    st_text = sp.text.rstrip()
                    if not st_text.strip() or st_text.lower().startswith("proposal"): break
                    buf.append(st_text)
                    if cur: cm.add(cur)
                    idx2 += 1
                bl = "\n".join(buf)
                props.append(bl)
                pcs[bl] = cm.copy()

    r = Document()
    r.add_heading("Proposal Summary", 0)

    if not props:
        r.add_paragraph("No proposals found.")
        bio = io.BytesIO()
        r.save(bio)
        bio.seek(0)
        return bio

    status_elem.text("Generating embeddings & Clustering...")
    em = TFIDFEmbedder()
    emb = em.encode(props)

    N = len(props)
    mn, mx = max(2, N // 5), max(3, N // 2)
    best_diff = float("inf")
    best_lbl = None
    for thr in np.linspace(0.2, 0.8, 13):
        try:
            hac = AgglomerativeClustering(
                n_clusters=None, metric="cosine", linkage="average",
                distance_threshold=thr, compute_full_tree=True,
            )
            lbls = hac.fit_predict(emb)
            cnt = len(set(lbls))
            diff = abs(cnt - (mn + mx) / 2)
            if diff < best_diff:
                best_diff = diff
                best_lbl = lbls
        except: pass
    lbls = best_lbl if best_lbl is not None else np.zeros(N, dtype=int)

    clusters = {}
    for i, l in enumerate(lbls):
        clusters.setdefault(l, {"idxs": [], "cm": set()})
        clusters[l]["idxs"].append(i)
        clusters[l]["cm"].update(pcs[props[i]])

    items = []
    for info in clusters.values():
        idxs = info["idxs"]
        subset = emb[idxs]
        cent = np.mean(subset, axis=0, keepdims=True)
        sims = cosine_similarity(cent, subset)[0]
        rep = props[idxs[int(np.argmax(sims))]]
        cm = sorted(info["cm"])
        items.append({"proposal": rep, "companies": cm, "count": len(cm)})

    items.sort(key=lambda x: x["count"], reverse=True)

    status_elem.text("Creating summary...")
    for it in items:
        r.add_paragraph(it["proposal"])
        r.add_paragraph(f"Supporting companies ({it['count']}): " + (", ".join(it["companies"]) if it["companies"] else "(none)"))
        r.add_paragraph("")

    bio = io.BytesIO()
    r.save(bio)
    bio.seek(0)
    log_func("Summary 생성 완료")
    return bio


# ==========================================
# 6. Output 3 — Gemini AI 분석 (선택, 서버 키)
#    ★ 할루시네이션 제로 + 정밀 그룹핑 프롬프트 ★
# ==========================================
def _build_doc_inventory(extracted_data):
    """다운로드된 문서 목록을 명시적으로 나열. Gemini가 이 목록 밖의 문서를 인용하면 할루시네이션."""
    lines = []
    for item in extracted_data:
        lines.append(f"  - {item['doc']} (회사: {item['company']})")
    return "\n".join(lines)


def run_gemini_analysis(extracted_data, status_elem, api_key):
    genai.configure(api_key=api_key)
    _gemini_start_time = time.time()  # 전체 소요 시간 추적

    # 문서 인벤토리 (허용된 문서 번호 목록)
    doc_inventory = _build_doc_inventory(extracted_data)
    valid_doc_ids = {item['doc'] for item in extracted_data}
    valid_companies = {item['company'] for item in extracted_data if item['company']}

    # 기고문 본문 구성
    text_buffer = []
    for item in extracted_data:
        text_buffer.append(
            f"========== 문서 시작: {item['doc']} ==========\n"
            f"회사: {item['company']}\n"
            f"제목: {item.get('title', '')}\n"
            f"내용:\n{item['content']}\n"
            f"========== 문서 끝: {item['doc']} =========="
        )
    full_text = "\n\n".join(text_buffer)

    # ★ 핵심 프롬프트 — 할루시네이션 방지 + 정밀 그룹핑 ★
    MAIN_PROMPT = f"""당신은 3GPP 표준화 회의 기고문을 분석하는 전문가입니다.

아래에 다운로드된 기고문 원문이 제공됩니다. 이 원문만을 근거로 분석하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[절대 규칙 — 할루시네이션 금지]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 아래 [허용된 문서 목록]에 있는 문서 번호만 인용할 수 있습니다.
   이 목록에 없는 문서 번호를 절대 만들어내거나 인용하지 마세요.
2. 아래 [허용된 회사 목록]에 있는 회사명만 사용할 수 있습니다.
   이 목록에 없는 회사를 절대 만들어내지 마세요.
3. 원문에 명시적으로 적혀 있는 내용만 분석하세요.
   원문에 없는 내용을 추론하거나 지어내지 마세요.
4. 어떤 회사가 어떤 제안을 지지하는지는, 해당 회사의 기고문에
   해당 제안이 실제로 기술되어 있을 때만 인정됩니다.
5. 확실하지 않으면 포함하지 마세요. 누락이 환각보다 낫습니다.

[허용된 문서 목록]
{doc_inventory}

[허용된 회사 목록]
{', '.join(sorted(valid_companies))}

주의: 다음 회사들은 같은 그룹이므로 하나의 회사로 취급하세요:
- ZTE = Sanechips (같은 그룹)
- Huawei = HiSilicon (같은 그룹)
- Nokia = Nokia Shanghai Bell (같은 그룹)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[그룹핑 규칙 — 정밀 그룹핑]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 두 제안을 같은 그룹으로 묶으려면, 다음 조건을 모두 만족해야 합니다:
   a) 같은 기술적 메커니즘을 다루고 있어야 합니다.
      (예: 둘 다 "C-DRX timer 제어"에 대해 이야기하는 경우)
   b) 제안하는 구체적 동작/방향이 동일하거나 매우 유사해야 합니다.
      (예: 둘 다 "network이 DRX timer를 제어해야 한다"고 제안하는 경우)
   c) 같은 주제를 다루더라도 제안 방향이 다르면 별도 그룹입니다.
      (예: "WUS 필요하다" vs "WUS 불필요하다"는 별도 그룹)

2. 뭉뚱그리지 마세요:
   - "에너지 효율 관련 제안"처럼 광범위한 그룹은 금지합니다.
   - "Cell DTX/DRX 패턴을 UE에 알려야 한다"처럼 구체적 동작 수준으로 묶으세요.
   - 하나의 기고문에 여러 개의 서로 다른 제안이 있으면,
     각 제안을 별도로 분류하세요 (같은 문서가 여러 그룹에 속할 수 있음).

3. 1개 회사만 단독 주장한 제안은 결과에서 제외하세요.
   반드시 2개 이상의 서로 다른 회사가 동일/유사 제안을 한 경우만 포함하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[출력 양식] — 반드시 아래 형식을 정확히 따르세요
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### [순위]. [제안의 구체적 동작을 요약한 제목]
* **지지 회사 (총 N개사):** 회사명1, 회사명2, ... (쉼표 구분, 중복 제거)
* **상세 내용:** 이 제안이 구체적으로 무엇을 요구하는지 2-3문장으로 기술. 원문의 표현을 최대한 유지.
* **근거 문서:**
  - [문서번호] (회사명): 해당 문서에서 이 제안이 나오는 부분의 핵심 문구 인용
  - [문서번호] (회사명): 해당 문서에서 이 제안이 나오는 부분의 핵심 문구 인용
  (각 지지 회사의 근거 문서를 반드시 하나 이상 나열하세요)

순위는 지지 회사 수가 많은 순서대로 내림차순으로 부여하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[기고문 원문 데이터]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{full_text}
"""

    MAP_PROMPT_TEMPLATE = """당신은 3GPP 기고문 분석 전문가입니다.

[절대 규칙]
1. 아래 제공된 문서에 있는 내용만 추출하세요. 없는 내용을 지어내지 마세요.
2. 허용된 문서 번호: {doc_list}
3. 각 제안을 추출할 때, 반드시 해당 제안이 적힌 문서 번호와 회사명을 함께 기록하세요.
4. 하나의 기고문에 여러 제안이 있으면 각각 별도로 추출하세요.
5. 구체적 동작 수준으로 추출하세요 (예: "Proposal 3: C-DRX with DL WUS" → 그대로 기록).

[출력 양식]
- 제안: [원문에 가깝게 제안 내용 기술]
- 문서: [문서번호]
- 회사: [회사명]
- 원문 근거: [해당 제안이 나오는 원문 문구를 최대한 그대로 인용]

빠지는 제안이 없도록 모든 Proposal, Observation, Recommendation을 추출하세요.

[기고문 원문]
{batch_text}"""

    REDUCE_PROMPT_TEMPLATE = """당신은 3GPP 기고문 분석 전문가입니다.

아래에 1차 추출된 제안 목록이 있습니다. 이를 최종 보고서로 병합하세요.

[절대 규칙 — 할루시네이션 금지]
1. 허용된 문서 번호: {doc_list}. 이 목록 밖의 문서를 인용하면 안 됩니다.
2. 허용된 회사: {company_list}. 이 목록 밖의 회사를 인용하면 안 됩니다.
3. 1차 추출 결과에 실제로 있는 내용만 사용하세요.

[그룹핑 규칙 — 정밀 그룹핑]
1. 구체적 동작이 동일한 제안만 같은 그룹으로 묶으세요.
2. 같은 주제라도 제안 방향이 다르면 별도 그룹입니다.
3. "에너지 효율" 같은 광범위한 그룹은 금지. "Cell DTX/DRX 패턴 정보를 UE에 전달" 수준으로 구체화.
4. 2개 이상의 회사가 지지하는 제안만 포함하세요.

[출력 양식]
### [순위]. [제안의 구체적 동작 요약 제목]
* **지지 회사 (총 N개사):** 회사1, 회사2, ...
* **상세 내용:** 구체적 제안 내용 2-3문장
* **근거 문서:**
  - [문서번호] (회사명): 원문 핵심 문구 인용
  - [문서번호] (회사명): 원문 핵심 문구 인용

지지 회사 수 내림차순으로 정렬.

[1차 추출 결과]
{intermediate_text}"""

    status_elem.text("🧠 Gemini AI 분석 중... 모델을 선택하고 있습니다...")
    try:
        valid_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]

        # Flash 모델 우선 사용 (무료 API 호환)
        target = next((m for m in valid_models if 'flash' in m.lower() and 'vision' not in m.lower()),
                       next((m for m in valid_models if 'pro' in m.lower() and 'vision' not in m.lower()), valid_models[-1]))

        model_display = target.split('/')[-1]
        model = genai.GenerativeModel(target)
        strict_config = {"temperature": 0.0}

        status_elem.text(f"🧠 모델: {model_display} — 분석을 시작합니다...")

        total_docs = len(extracted_data)
        response = None
        doc_list_str = ", ".join(sorted(valid_doc_ids))
        company_list_str = ", ".join(sorted(valid_companies))

        if total_docs > 20:
            # Map-Reduce
            batch_size = 20
            total_batches = (total_docs + batch_size - 1) // batch_size
            intermediate = []
            for i in range(total_batches):
                status_elem.text(f"🚀 1차 추출 [{i+1}/{total_batches}]")
                batch = extracted_data[i*batch_size:(i+1)*batch_size]
                bt = "\n\n".join([
                    f"========== {it['doc']} ({it['company']}) ==========\n{it['content']}"
                    for it in batch
                ])
                batch_docs = ", ".join([it['doc'] for it in batch])
                mp = MAP_PROMPT_TEMPLATE.format(doc_list=batch_docs, batch_text=bt)

                batch_success = False
                for attempt in range(5):
                    try:
                        res = model.generate_content(mp, generation_config=strict_config)
                        try:
                            if res and res.text and len(res.text.strip()) > 10:
                                intermediate.append(res.text)
                                batch_success = True
                        except (ValueError, AttributeError):
                            # Safety filter로 차단되거나 응답이 비정상인 경우
                            append_log(f"배치 {i+1}: 응답 텍스트 접근 실패 (safety filter?)")
                        break
                    except Exception as e:
                        if "429" in str(e) or "503" in str(e):
                            wait = 60 * (attempt + 1)
                            elapsed = int(time.time() - _gemini_start_time)
                            if elapsed > 600:  # 10분 초과
                                status_elem.text(
                                    f"⚠️ {elapsed//60}분 경과 — API 과부하가 심합니다. "
                                    f"'내 개인 API 키'를 사용하면 즉시 분석 가능합니다."
                                )
                            for cd in range(wait,0,-1):
                                elapsed = int(time.time() - _gemini_start_time)
                                status_elem.text(
                                    f"⚠️ API 과부하 대기 {cd}초 (시도 {attempt+1}/5, "
                                    f"총 {elapsed//60}분 {elapsed%60}초 경과)"
                                )
                                time.sleep(1)
                        else: raise

                if not batch_success:
                    append_log(f"배치 {i+1} 실패 (5회 재시도 소진)")

                if i < total_batches-1:
                    for cd in range(5,0,-1):
                        status_elem.text(f"⏳ 배치 간 대기 {cd}초 ({i+1}/{total_batches} 완료)")
                        time.sleep(1)

            if not intermediate:
                elapsed = int(time.time() - _gemini_start_time)
                st.error(
                    f"❌ **{elapsed//60}분 동안 시도했으나 결과를 받지 못했습니다.**\n\n"
                    f"동시 사용자가 많아 서버 기본 API 한도가 소진된 상태입니다.\n\n"
                    f"**해결 방법:** 위에서 **'🔐 내 개인 Gemini API 키 사용'**을 선택하세요. "
                    f"1분이면 무료 발급 가능하고, 자기만의 한도로 바로 분석됩니다."
                )
                return False

            status_elem.text("🧠 최종 병합 분석 중... (가장 오래 걸리는 단계입니다)")
            fi = "\n\n=== 배치 구분 ===\n\n".join(intermediate)
            rp = REDUCE_PROMPT_TEMPLATE.format(
                doc_list=doc_list_str,
                company_list=company_list_str,
                intermediate_text=fi,
            )
            for attempt in range(5):
                try:
                    response = model.generate_content(rp, generation_config=strict_config); break
                except Exception as e:
                    if "429" in str(e) or "503" in str(e):
                        wait = 60 * (attempt + 1)
                        for cd in range(wait,0,-1):
                            elapsed = int(time.time() - _gemini_start_time)
                            status_elem.text(f"⚠️ 최종 병합 대기 {cd}초 (시도 {attempt+1}/5, 총 {elapsed//60}분 {elapsed%60}초 경과)"); time.sleep(1)
                    else: raise
        else:
            # Direct analysis
            for attempt in range(5):
                try:
                    response = model.generate_content(MAIN_PROMPT, generation_config=strict_config); break
                except Exception as e:
                    if "429" in str(e) or "503" in str(e):
                        wait = 60 * (attempt + 1)
                        elapsed = int(time.time() - _gemini_start_time)
                        if elapsed > 600:
                            status_elem.text(
                                f"⚠️ {elapsed//60}분 경과 — '내 개인 API 키'를 사용하면 즉시 분석 가능합니다."
                            )
                            time.sleep(3)
                        for cd in range(wait,0,-1):
                            elapsed = int(time.time() - _gemini_start_time)
                            status_elem.text(f"⚠️ API 대기 {cd}초 (시도 {attempt+1}/5, 총 {elapsed//60}분 {elapsed%60}초 경과)"); time.sleep(1)
                    else: raise

        status_elem.text("🔍 응답 확인 중...")

        # response 검증
        if response is None:
            st.error(
                "❌ **API 응답을 받지 못했습니다.**\n\n"
                "동시 사용자가 너무 많아 서버 기본 API의 한도가 소진된 상태입니다.\n\n"
                "**해결 방법:** 위에서 **'🔐 내 개인 Gemini API 키 사용'**을 선택하고, "
                "개인 무료 API 키를 입력하면 자기만의 한도로 바로 분석할 수 있습니다. "
                "(발급 가이드가 화면에 표시됩니다)"
            )
            return False

        if not hasattr(response, 'text'):
            st.error(
                "❌ **API가 빈 응답을 반환했습니다.**\n\n"
                "서버 과부하 또는 문서가 너무 많아 처리에 실패했습니다.\n\n"
                "**해결 방법:** '🔐 내 개인 Gemini API 키 사용'으로 전환하거나, "
                "분석할 문서 수를 줄여서 다시 시도해 주세요."
            )
            return False

        try:
            result_text = response.text
        except (ValueError, AttributeError) as e:
            st.error(
                "❌ **AI 응답이 안전 필터에 의해 차단되었습니다.**\n\n"
                "기고문 내용이 AI 안전 정책에 의해 필터링되었을 수 있습니다.\n\n"
                "**해결:** 다시 시도하거나 NotebookLM을 사용해 주세요."
            )
            append_log(f"Gemini safety filter: {e}")
            return False

        if not result_text or len(result_text.strip()) < 50:
            st.error(
                "❌ **AI 응답이 너무 짧습니다.**\n\n"
                "API 부하로 인해 불완전한 응답이 왔습니다.\n\n"
                "**해결 방법:** '🔐 내 개인 Gemini API 키 사용'으로 전환하면 "
                "자기만의 한도로 안정적으로 분석할 수 있습니다."
            )
            return False

        status_elem.text("✅ AI 분석 완료! 결과 문서를 생성하고 있습니다...")

        # ★ 할루시네이션 후처리 검증 ★
        cited_docs = set(re.findall(r'[A-Z]\d?-\d{7}', result_text))
        hallucinated = cited_docs - valid_doc_ids
        if hallucinated:
            result_text += f"\n\n---\n⚠️ **검증 경고:** 다음 문서 번호는 다운로드된 파일 목록에 없습니다 (할루시네이션 가능성): {', '.join(sorted(hallucinated))}"

        # Output 3 docx 생성
        doc = Document()
        doc.add_heading(f"AI 정밀 분석 요약 ({model_display})", 0)
        doc.add_paragraph(f"분석 대상: {total_docs}개 문서")
        doc.add_paragraph(f"분석 모델: {model_display} (temperature=0.0)")
        doc.add_paragraph("")
        for line in result_text.split('\n'):
            if re.match(r'^(#+)?\s*\d+\.|^###', line.strip()):
                p = doc.add_paragraph()
                p.add_run(line.replace('#','').strip()).bold = True
            elif line.strip().startswith('* **'):
                doc.add_paragraph(line.strip())
            elif line.strip().startswith('- [') or line.strip().startswith('- '):
                doc.add_paragraph(line.strip())
            else:
                doc.add_paragraph(line)
        bio = io.BytesIO()
        doc.save(bio)
        st.session_state.ai_summary_bytes = bio.getvalue()
        st.session_state.ai_summary_text = result_text
        st.session_state.ai_model_name = model_display
        st.session_state.ai_summary_generated = True
        status_elem.text("✅ 완료! 아래에서 결과를 확인하세요.")
        st.rerun()

    except Exception as e:
        err = str(e)
        # ★ 보안: 에러 메시지에서 API 키가 노출되지 않도록 필터링
        if GEMINI_API_KEY and GEMINI_API_KEY in err:
            err = err.replace(GEMINI_API_KEY, "***HIDDEN***")
        if api_key_to_use and api_key_to_use in err:
            err = err.replace(api_key_to_use, "***HIDDEN***")
        if "429" in err or "Quota" in err or "exhausted" in err.lower():
            st.error(
                "❌ **API 용량이 완전히 소진되었습니다.**\n\n"
                "현재 서버 기본 API의 일일 한도가 초과되었거나, 동시 사용자가 너무 많습니다.\n\n"
                "**해결 방법:** 위에서 **'🔐 내 개인 Gemini API 키 사용'**을 선택하세요. "
                "개인 무료 API 키를 발급받으면 (1분 소요, 완전 무료) "
                "자기만의 한도(일 1,500회)로 바로 분석할 수 있습니다."
            )
        else:
            st.error(
                f"❌ **API 오류가 발생했습니다.**\n\n"
                f"잠시 후 다시 시도하거나, '내 개인 API 키'를 사용해 보세요."
            )
            append_log(f"Gemini error (sanitized): {err[:200]}")
    return False


# ==========================================
# 7. Streamlit UI
# ==========================================
st.sidebar.title("📡 3GPP Analyzer v2")
st.sidebar.caption("기본 분석 + Gemini AI 강화")
st.sidebar.markdown("---")
page = st.sidebar.radio("메뉴", ["🚀 통합 분석기", "⚙️ 설정", "ℹ️ 가이드"])

# ─── Settings ───
if page == "⚙️ 설정":
    st.title("⚙️ 서버 설정")

    st.subheader("🔒 보안 안내")
    st.info(
        "API 키와 Cloud Function URL은 서버 환경변수 또는 Streamlit Secrets에 저장되며, "
        "사용자 브라우저에 노출되지 않습니다. 에러 발생 시에도 키 값이 화면에 표시되지 않습니다."
    )

    st.subheader("Gemini API Key")
    st.info(f"상태: {'✅ 설정됨 (서버에 안전하게 저장)' if GEMINI_API_KEY else '❌ 미설정'}")
    if not GEMINI_API_KEY:
        st.code('# .streamlit/secrets.toml\nGEMINI_API_KEY = "AIzaSy..."', language="toml")

    st.subheader("Cloud Function URL")
    if CLOUD_FUNCTION_URL:
        # URL도 전체를 보여주지 않음
        masked_url = CLOUD_FUNCTION_URL[:40] + "..." if len(CLOUD_FUNCTION_URL) > 40 else CLOUD_FUNCTION_URL
        st.info(f"상태: ✅ 설정됨 ({masked_url})")
    else:
        st.info("상태: ⚠️ 미설정 (서버에서 직접 다운로드)")

elif page == "ℹ️ 가이드":
    st.title("ℹ️ 사용 가이드")

    # ── 기본 사용법 ──
    st.header("🔰 기본 사용법")
    st.markdown("""
**1단계:** 🔍 회의 번호로 자동 조회 → Working Group과 회의 번호 입력 → Agenda 불러오기

**2단계:** 📋 Agenda 선택 → 🚀 기본 분석 실행
- **Output 1:** 각 기고문의 결론(Conclusion) 부분을 서식 그대로 취합한 문서
- **Output 2:** TF-IDF 단어 빈도 분석으로 유사한 Proposal을 자동 그룹핑한 요약

**3단계:** ✨ Gemini AI 정밀 분석 (선택)
- AI가 의미 기반으로 제안을 그룹핑하고, 지지 회사별로 정렬
- 각 제안에 대해 원문 근거 문서와 인용 제공
    """)

    # ── Gemini API 발급 가이드 ──
    st.markdown("---")
    st.header("🔑 Gemini API 키 발급 가이드")

    st.subheader("🟢 무료 API 키 발급 (추천, 1분 소요)")
    st.markdown("""
**누구나 무료**로 발급받을 수 있으며, **카드 등록이 필요 없습니다.**

**1단계:** 아래 링크를 클릭하여 Google AI Studio에 접속합니다 (구글 로그인 필요):

👉 **[Google AI Studio - API 키 발급 페이지](https://aistudio.google.com/app/apikey)**

**2단계:** 화면에서 파란색 **`Create API key`** 버튼을 클릭합니다.

**3단계:** 팝업이 뜨면 **`Create API key in new project`** 를 클릭합니다.

> ⚠️ **중요:** 반드시 **"in new project"**를 선택하세요!
> 이렇게 하면 결제 계정이 연결되지 않은 별도 프로젝트에 키가 생성되어, **절대 과금되지 않습니다.**
> 무료 한도(일 1,500회)를 초과하면 그냥 에러가 나고 끝입니다.

**4단계:** `AIzaSy...` 로 시작하는 긴 문자열이 생성됩니다. 이것이 API 키입니다.

**5단계:** 이 문자열을 **복사(Ctrl+C)**하여 분석기의 API 키 입력창에 **붙여넣기(Ctrl+V)**하세요.

**무료 한도:**
- **gemini-2.0-flash:** 분당 15회, 일 1,500회
- 1회 분석에 약 1~3회 API 호출 → **하루 수백 회 분석 가능**
- 한도를 초과하면 과금 없이 그냥 에러가 납니다 (과금 절대 불가)
    """)

    # ── 분석 결과 설명 ──
    st.markdown("---")
    st.header("📊 분석 결과 설명")
    st.markdown("""
**Output 1 (Conclusions 취합):**
- 각 기고문에서 Conclusion/Summary 섹션만 추출
- 원문의 서식(Bold, 폰트 등)을 그대로 보존
- 문서 번호, 회사명, 제목, 링크가 표 형태로 정리

**Output 2 (TF-IDF 요약):**
- 단어 빈도(TF-IDF) 기반으로 유사한 Proposal을 자동 클러스터링
- 지지 회사 수가 많은 순서대로 정렬
- AI를 사용하지 않으므로 항상 즉시 결과 생성

**Output 3 (Gemini AI 정밀 분석):**
- AI가 의미 기반으로 제안의 기술적 유사성을 판단
- 각 제안에 대해 근거 문서와 원문 인용 제공
- 할루시네이션 방지 시스템으로 검증된 문서만 인용
    """)

    # ── 보안 안내 ──
    st.markdown("---")
    st.header("🔒 개인정보 및 보안")
    st.markdown("""
- **API 키 보호:** 입력한 API 키는 화면에 `****` 형태로 가려지며, 서버에 저장되지 않습니다.
- **에러 메시지 보안:** 오류 발생 시에도 API 키가 화면에 표시되지 않도록 자동 마스킹됩니다.
- **문서 데이터:** 다운로드된 기고문은 분석 완료 즉시 서버에서 자동 삭제됩니다.
- **세션 종료:** 브라우저를 닫으면 모든 데이터가 즉시 소멸됩니다.
    """)

# ─── Main ───
elif page == "🚀 통합 분석기":
    st.title("🚀 3GPP 기고문 통합 분석기")
    st.caption("Output 1·2는 기본 | Output 3 Gemini는 선택")

    # Step 1: Input
    st.header("1️⃣ 데이터 입력")
    if CLOUD_FUNCTION_URL:
        st.success("☁️ Cloud Function 연결됨 — 클라우드에서 처리합니다.")

    input_method = st.radio(
        "입력 방식:",
        ("🔍 회의 번호로 자동 조회", "Excel 파일 업로드", "링크 직접 입력"),
        horizontal=True,
    )
    entries = []

    if input_method == "🔍 회의 번호로 자동 조회":
        col_wg, col_num = st.columns([1, 2])

        with col_wg:
            wg = st.selectbox("Working Group:", list(WG_FTP_MAP.keys()))

        with col_num:
            meeting_num_input = st.text_input(
                "회의 번호 입력 (예: 133bis, 122, 168):",
                placeholder="133bis",
                help="3GPP 회의 번호만 입력. 예: RAN2#133bis → '133bis' 입력"
            )

        if meeting_num_input and meeting_num_input.strip():
            meeting_num = meeting_num_input.strip()

            if st.button("📋 Agenda 불러오기", type="primary"):
                with st.spinner(f"{wg}#{meeting_num} 폴더 검색 및 TDoc 리스트 다운로드 중... (3GPP 서버가 느릴 수 있습니다)"):
                    # 실제 폴더명 찾기 (SA2: 168 → TSGS2_168_Goteborg_2025-04)
                    meeting_folder = resolve_meeting_folder(wg, meeting_num)
                    if meeting_folder:
                        st.session_state["resolved_folder"] = meeting_folder
                        agenda_dict, all_entries = fetch_tdoc_list_xlsx(wg, meeting_folder)
                        st.session_state.agenda_dict = agenda_dict
                        st.session_state.all_entries = all_entries
                        if not agenda_dict:
                            st.error(
                                f"❌ **TDoc 리스트를 찾지 못했습니다.**\n\n"
                                f"폴더 `{meeting_folder}`는 존재하지만 xlsx 파일을 다운로드하지 못했습니다.\n\n"
                                f"**원인:** 3GPP 서버가 일시적으로 느리거나, 해당 회의의 TDoc 리스트가 아직 업로드되지 않았을 수 있습니다.\n\n"
                                f"**해결:** 잠시 후 다시 시도하거나, 'Excel 파일 업로드' 방식을 사용하세요."
                            )
                    else:
                        st.error(
                            f"❌ **{wg}#{meeting_num}에 해당하는 폴더를 찾지 못했습니다.**\n\n"
                            f"**가능한 원인:**\n"
                            f"- 회의 번호가 잘못되었을 수 있습니다\n"
                            f"- 3GPP 서버가 일시적으로 응답하지 않습니다\n\n"
                            f"**해결:** 번호를 확인하고 다시 시도하거나, 잠시 후 재시도하세요."
                        )
                        st.session_state.agenda_dict = {}
                        st.session_state.all_entries = []

            if st.session_state.get("resolved_folder"):
                st.caption(f"📂 `ftp/{WG_FTP_MAP.get(wg, '')}/{st.session_state.get('resolved_folder', '')}/Docs/`")

        if st.session_state.agenda_dict:
            agenda_items = sorted(st.session_state.agenda_dict.keys())
            st.success(f"✅ {len(agenda_items)}개 agenda, 총 {len(st.session_state.all_entries)}개 문서 발견")

            st.markdown("#### 👇 분석할 Agenda를 선택하세요")
            selected_agenda = st.selectbox(
                "Agenda 선택:",
                agenda_items,
                format_func=lambda x: f"{x} ({len(st.session_state.agenda_dict[x])}개 문서)",
                label_visibility="collapsed",
            )

            if selected_agenda:
                entries = st.session_state.agenda_dict[selected_agenda]
                st.session_state["selected_agenda_name"] = selected_agenda
                st.info(f"📄 **{selected_agenda}** — {len(entries)}개 문서가 분석 대상입니다.")

                with st.expander(f"문서 목록 미리보기 ({len(entries)}개)", expanded=False):
                    for e in entries[:30]:
                        st.text(f"  {e['doc']}  |  {e['company']}")
                    if len(entries) > 30:
                        st.caption(f"  ... 외 {len(entries)-30}개")

    elif input_method == "Excel 파일 업로드":
        uploaded = st.file_uploader("엑셀(.xlsx) — 1열: 문서번호(하이퍼링크), 3열: 회사명", type=["xlsx","xls"])
        if uploaded:
            entries = read_excel_from_bytes(uploaded)
            st.info(f"총 {len(entries)}개 문서 인식")
    else:
        raw = st.text_area("3GPP .zip 링크를 한 줄에 하나씩:", height=120)
        if raw:
            for line in raw.strip().split("\n"):
                url = line.strip()
                if url:
                    docid = url.split("/")[-1].replace(".zip","")
                    entries.append({"doc": docid, "company": "Unknown", "link": url})
            st.info(f"총 {len(entries)}개 문서 인식")

    # Step 2: 기본 분석
    st.markdown("---")
    st.header("2️⃣ 기본 분석 (Output 1 + 2)")
    st.write("결론(Conclusions) 추출 + TF-IDF 기반 Proposal 요약을 생성합니다.")

    if st.button("🚀 기본 분석 실행 (Run)", type="primary", use_container_width=True):
        if not entries:
            st.warning("먼저 데이터를 입력해주세요.")
        else:
            st.session_state.log_text = ""
            st.session_state.process_done = False
            st.session_state.ai_summary_generated = False
            st.session_state.ai_summary_bytes = None

            # 시각적 진행률 표시
            progress_container = st.container()
            with progress_container:
                st.subheader("📊 처리 진행 상황")
                progress_bar = st.progress(0)
                status_text = st.empty()
                step_detail = st.empty()

                # Phase 1: 다운로드 & 결론 추출
                status_text.markdown("**📥 Phase 1/2:** 기고문 다운로드 및 결론 추출")
                step_detail.caption(f"총 {len(entries)}개 문서를 3GPP 서버에서 다운로드합니다...")
                out1_bio = extract_all_conclusions(entries, step_detail, progress_bar, append_log)

                # Phase 2: TF-IDF 요약
                status_text.markdown("**🔬 Phase 2/2:** TF-IDF 기반 제안 클러스터링")
                step_detail.caption("단어 빈도 분석으로 유사한 제안을 자동 그룹핑합니다...")
                out2_bio = parse_and_summarize(out1_bio, step_detail, append_log)

                progress_bar.progress(1.0)
                status_text.markdown("**✅ 기본 분석 완료!**")
                step_detail.empty()

            st.session_state.out1_bytes = out1_bio.getvalue()
            st.session_state.out2_bytes = out2_bio.getvalue()
            st.session_state.process_done = True

    if st.session_state.process_done:
        st.success("🎉 기본 분석 완료! Output 1·2를 다운로드하세요.")

        # 파일명에 agenda 정보 포함
        agenda_tag = _safe_filename(st.session_state.get("selected_agenda_name", ""), 30)
        if not agenda_tag:
            agenda_tag = "manual"

        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.out1_bytes:
                st.download_button("📥 Output 1 (Conclusions 취합.docx)",
                    data=st.session_state.out1_bytes,
                    file_name=f"output1_conclusions_{agenda_tag}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True)
        with col2:
            if st.session_state.out2_bytes:
                st.download_button("📥 Output 2 (TF-IDF 요약.docx)",
                    data=st.session_state.out2_bytes,
                    file_name=f"output2_summary_tfidf_{agenda_tag}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True)

        # ══════════════════════════════════════════════
        # Step 3: Gemini AI 정밀 분석 (NotebookLM 위에 배치)
        # ══════════════════════════════════════════════
        st.markdown("---")
        st.header("3️⃣ AI 정밀 분석 (Gemini)")
        st.write("추출된 결론을 Gemini AI로 의미 분석하여 더 정확한 제안 그룹핑 및 회사 정렬을 생성합니다.")

        st.warning(
            "⏱️ **소요 시간 안내:** 무료 Gemini API는 분당 처리량이 제한되어 있습니다. "
            "문서 수에 따라 **약 5~15분** 이상 소요될 수 있으며, API 과부하 시 자동으로 "
            "대기 후 재시도합니다. **분석이 끝날 때까지 페이지를 닫지 마시고 느긋하게 기다려 주세요.**"
        )

        # API 키 선택: 서버 키 vs 개인 키
        api_key_to_use = None

        if GEMINI_API_KEY:
            key_mode = st.radio(
                "API 키 선택:",
                ("🔑 서버 기본 키 사용 (별도 설정 불필요)", "🔐 내 개인 Gemini API 키 사용"),
                horizontal=True,
                help="서버 기본 키의 일일 한도가 초과된 경우, 개인 무료 API 키를 입력하여 사용할 수 있습니다."
            )
            if "개인" in key_mode:
                with st.expander("📖 개인 API 키 발급 방법 (1분이면 끝! 완전 무료)", expanded=True):
                    st.markdown("""
**컴퓨터 초보도 따라할 수 있는 3단계 가이드:**

**1단계:** 아래 링크를 클릭하세요 (구글 로그인 필요):
👉 **[Google AI Studio - API 키 발급 페이지](https://aistudio.google.com/app/apikey)**

**2단계:** 화면에서 파란색 **'Create API key'** 버튼을 클릭하세요.
→ 팝업이 뜨면 **'Create API key in new project'** 를 클릭하세요.
→ ⚠️ **반드시 'in new project'를 선택하세요!** 이렇게 하면 과금이 절대 되지 않습니다.

**3단계:** `AIzaSy...` 로 시작하는 긴 문자가 생성됩니다.
→ 이 문자를 **복사(Ctrl+C)**하여 아래 입력창에 **붙여넣기(Ctrl+V)**하세요.

✅ **완전 무료**입니다. 카드 등록이 필요 없고, 하루 1,500회까지 무료로 사용 가능합니다.
                    """)

                personal_key = st.text_input(
                    "개인 Gemini API Key 입력:",
                    type="password",
                    placeholder="AIzaSy...",
                )
                if personal_key and personal_key.strip():
                    api_key_to_use = personal_key.strip()
                else:
                    st.caption("⬆️ 위에 개인 API 키를 입력하세요.")
            else:
                api_key_to_use = GEMINI_API_KEY
        else:
            st.info("서버에 기본 API 키가 설정되어 있지 않습니다. 개인 Gemini API 키를 입력해주세요.")

            with st.expander("📖 API 키 발급 방법 (1분이면 끝! 완전 무료)", expanded=True):
                st.markdown("""
**컴퓨터 초보도 따라할 수 있는 3단계 가이드:**

**1단계:** 아래 링크를 클릭하세요 (구글 로그인 필요):
👉 **[Google AI Studio - API 키 발급 페이지](https://aistudio.google.com/app/apikey)**

**2단계:** 화면에서 파란색 **'Create API key'** 버튼을 클릭하세요.
→ 팝업이 뜨면 **'Create API key in new project'** 를 클릭하세요.
→ ⚠️ **반드시 'in new project'를 선택하세요!** 이렇게 하면 과금이 절대 되지 않습니다.

**3단계:** `AIzaSy...` 로 시작하는 긴 문자가 생성됩니다.
→ 이 문자를 **복사(Ctrl+C)**하여 아래 입력창에 **붙여넣기(Ctrl+V)**하세요.

✅ **완전 무료**입니다. 카드 등록이 필요 없고, 하루 1,500회까지 무료로 사용 가능합니다.
                """)

            personal_key = st.text_input(
                "Gemini API Key 입력:",
                type="password",
                placeholder="AIzaSy...",
            )
            if personal_key and personal_key.strip():
                api_key_to_use = personal_key.strip()

        if api_key_to_use:

            st.markdown("")
            st.markdown("#### 👇 준비가 되었으면 아래 버튼을 클릭하세요")
            if st.button("✨ Gemini AI 정밀 분석 시작", use_container_width=True, type="primary"):
                gemini_container = st.container()
                with gemini_container:
                    st.subheader("🧠 Gemini AI 분석 진행 상황")
                    gemini_progress = st.progress(0)
                    gemini_status = st.empty()
                    gemini_detail = st.empty()

                    total_docs = len(st.session_state.extracted_data)
                    if total_docs > 20:
                        total_batches = (total_docs + 19) // 20
                        gemini_detail.caption(
                            f"📋 {total_docs}개 문서를 {total_batches}개 그룹으로 나누어 분석합니다. "
                            f"무료 API 기준 약 {max(5, total_batches * 2)}~{max(10, total_batches * 3 + 5)}분 소요 예상. "
                            f"끝날 때까지 이 페이지를 닫지 마세요."
                        )
                    else:
                        gemini_detail.caption(
                            f"📋 {total_docs}개 문서를 일괄 분석합니다. 약 5~10분 소요 예상. "
                            f"끝날 때까지 이 페이지를 닫지 마세요."
                        )

                    run_gemini_analysis(st.session_state.extracted_data, gemini_status, api_key_to_use)

        # Gemini 결과 표시
        if st.session_state.ai_summary_generated:
            st.success("✅ AI 정밀 요약 완료!")
            st.download_button(
                f"📥 Output 3 (AI 정밀 요약 - {st.session_state.ai_model_name}.docx)",
                data=st.session_state.ai_summary_bytes,
                file_name=f"Output3_AI_Summary_{agenda_tag}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                type="primary", use_container_width=True)
            with st.expander("👀 AI 분석 결과 미리보기", expanded=True):
                st.markdown(st.session_state.ai_summary_text)

        # ══════════════════════════════════════════════
        # Step 4: NotebookLM 활용 가이드 (Gemini 아래 배치)
        # ══════════════════════════════════════════════
        st.markdown("---")
        st.header("4️⃣ Google NotebookLM 활용하기 (대안)")
        st.success(
            "💡 **환각(Hallucination) 제로!** NotebookLM은 오직 업로드한 문서 기반으로만 "
            "답변을 생성하여 압도적인 정확도를 자랑합니다. Gemini 분석의 대안 또는 보완으로 활용하세요."
        )

        col_a, col_b = st.columns([2, 1])
        with col_a:
            st.markdown("""
**[NotebookLM의 압도적 장점]**
* **제한 없는 속도 & 무료:** 복잡한 API 키 발급이나 토큰 초과(429) 에러 없이 **완전 무료**로 즉시 사용 가능!
* **초대용량 지원:** 노트북 당 **최대 50개의 파일**, 파일당 **최대 50만 단어(약 2,500만 자)**까지 한 번에 거뜬히 분석.
* **투명한 출처 표기:** 요약된 문장이 원문 기고문의 어느 회사의 어떤 부분인지 정확히 짚어주는 인용(Citation) 링크 제공.
            """)
        with col_b:
            if st.session_state.notebooklm_txt:
                st.download_button(
                    label="📝 NotebookLM 전용 텍스트(.txt) 다운로드",
                    data=st.session_state.notebooklm_txt.encode('utf-8'),
                    file_name=f"NotebookLM_Conclusions_{agenda_tag}.txt",
                    mime="text/plain",
                    type="primary",
                    use_container_width=True,
                )

        st.markdown("---")
        st.markdown("#### 📋 1분 만에 끝내는 NotebookLM 완벽 요약 가이드")
        st.markdown("1. 위 버튼을 눌러 **텍스트 파일(.txt)**을 내 PC에 저장합니다.")
        st.markdown("2. 👉 **[Google NotebookLM 공식 사이트](https://notebooklm.google.com/)** 에 접속하여 로그인합니다.")
        st.markdown("3. 화면의 **'새 노트북(New Notebook)'** 버튼을 누르고, 좌측 소스 탭에 방금 받은 `.txt` 파일을 끌어다 놓습니다.")

        st.error(
            "🚨 **[중요] 무한 로딩 현상 대처 꿀팁:** 파일 업로드 후, 우측 패널에서 파일명 옆에 "
            "체크표시(✅)가 안 뜨고 **계속 빙글빙글 돌며 무한 로딩**이 걸리는 경우가 종종 있습니다. "
            "이는 화면상 표기 버그일 뿐 실제로는 분석이 끝난 상태입니다! 당황하지 마시고 "
            "**그냥 무시한 채로 바로 아래 채팅창에 질문을 전송**하시거나, **F5(새로고침)를 한 번 눌러주시면** 정상 작동합니다."
        )

        st.markdown("4. 화면 하단 채팅창에 아래의 **구조화된 누락 방지 전문가용 프롬프트**를 복사하여 붙여넣고 전송(Enter)하면 완벽한 포맷의 요약이 도출됩니다!")

        notebooklm_prompt = """당신은 3GPP 표준화 회의의 전문 기술 분석가입니다.
제공된 모든 기고문 전체 원문 모음을 꼼꼼히 검토하고, 아래의 [분석 지침]과 [출력 양식]을 엄격하게 준수하여 분석 보고서를 작성해 주세요.

[분석 지침]
1. 필터링: 반드시 "2개 이상의 회사"가 공통으로 지지하거나 유사한 기술적 주장을 하는 제안(Proposal)만 추출하세요. (1개 회사만 단독으로 주장한 내용은 완전히 제외합니다.)
2. 그룹화: 단어 형태가 달라도 '기술적 핵심 의미와 목적'이 동일하다면 하나의 그룹으로 묶어주세요.
3. 정렬: 지지하는 회사 수가 가장 많은 제안 그룹부터 '내림차순'으로, 2개 이상의 회사가 지지하는 제안(proposal)들을 모두 정렬하세요.
4. 제약사항: 오직 제공된 소스 문서에 명시된 내용, 회사명, 문서 번호만 사용하고, 절대 외부 지식을 섞거나 지어내지 마세요. 환각(Hallucination)을 엄격히 금지합니다.

[출력 양식] (반드시 아래의 마크다운 양식을 똑같이 복제하여 출력할 것)
### [순위]. [제안의 핵심 요약 제목]
* 지지 회사 (총 N개사): [회사명1, 회사명2, ...] (중복 제거 후 쉼표로 나열)
* 상세 제안 내용: [해당 제안의 기술적 배경과 핵심 요구사항을 2~3문장으로 명확하고 이해하기 쉽게 요약]
* 관련 문서 번호: [해당 제안이 포함된 원문 기고문 번호들 (예: R1-2600126 등)]"""
        st.code(notebooklm_prompt, language="text")

    # ── Log ──
    with st.expander("📝 처리 로그", expanded=False):
        st.text(st.session_state.log_text)
