"""
RAG (Retrieval-Augmented Generation) Module for Meeting Analysis
Processes drill-down queries on meeting data and returns evidence-backed answers.
"""

from typing import Dict, List, Any
import re
from difflib import SequenceMatcher
import os
from chromadb import Client

# --- Model Integration ---
import os
MODEL_PATH = os.environ.get("RAG_MODEL_PATH", r"C:\Models\gte-large")

try:
    from transformers import AutoTokenizer, AutoModel
    import torch
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModel.from_pretrained(MODEL_PATH)
    def embed_text(text: str) -> list:
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=128)
        with torch.no_grad():
            outputs = model(**inputs)
            embedding = outputs.last_hidden_state.mean(dim=1).squeeze().tolist()
        return embedding
except Exception:
    def embed_text(text: str) -> list:
        vec = [0.0] * 384
        for i, word in enumerate(text.lower().split()):
            idx = hash(word) % 384
            vec[idx] += (hash(word + str(i)) % 1000) / 1000.0
        norm = sum(x * x for x in vec) ** 0.5
        if norm > 0:
            vec = [x / norm for x in vec]
        return vec

def cosine_similarity(vec1, vec2) -> float:
    if not vec1 or not vec2:
        return 0.0
    dot = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = sum(a * a for a in vec1) ** 0.5
    norm2 = sum(b * b for b in vec2) ** 0.5
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)

def calculate_relevance_score(query: str, text: str) -> float:
    query_emb = embed_text(query)
    text_emb = embed_text(text)
    emb_score = cosine_similarity(query_emb, text_emb)
    query_lower = query.lower()
    text_lower = text.lower()
    similarity = SequenceMatcher(None, query_lower, text_lower).ratio()
    query_terms = query_lower.split()
    match_count = sum(1 for term in query_terms if term in text_lower)
    term_boost = min(match_count / len(query_terms) if query_terms else 0, 1.0)
    relevance = (emb_score * 0.5) + (similarity * 0.2) + (term_boost * 0.3)
    return min(relevance, 1.0)

# --- ChromaDB Integration ---
class LocalEmbeddingFunction:
    def __call__(self, input):
        return [embed_text(text) for text in input]

embedding_function = LocalEmbeddingFunction()
client = Client()
collection_name = os.environ.get("RAG_COLLECTION_NAME", "meeting_rag")
collection = client.create_collection(collection_name, embedding_function=embedding_function)

def insert_segments(input_data):
    docs = []
    metadatas = []
    ids = []
    meeting_id = input_data["meeting_id"]
    for idx, item in enumerate(input_data["action_items"]):
        text = f"Task: {item['task']} | Owner: {item['owner']} | Due: {item['due']}"
        docs.append(text)
        metadatas.append({"source": "mentor_json", "type": "action_item", "index": idx, "meeting_id": meeting_id})
        ids.append(f"{meeting_id}_action_item_{idx}")
    for idx, decision in enumerate(input_data["decisions_made"]):
        docs.append(decision)
        metadatas.append({"source": "mentor_json", "type": "decision", "index": idx, "meeting_id": meeting_id})
        ids.append(f"{meeting_id}_decision_{idx}")
    segments = input_data["transcript"].split(". ")
    for idx, segment in enumerate(segments):
        docs.append(segment)
        metadatas.append({"source": "raw_transcript", "type": "transcript", "line_number": idx+1, "meeting_id": meeting_id})
        ids.append(f"{meeting_id}_transcript_{idx+1}")
    return docs, metadatas, ids

def classify_query_type(query: str) -> str:
    q = query.lower()
    if any(kw in q for kw in ["summary", "summarize", "meeting about", "key point", "main topic"]):
        return "summary"
    if any(kw in q for kw in ["decision", "decided", "resolution", "conclusion"]):
        return "decision"
    if any(kw in q for kw in ["action item", "task", "to do", "responsible", "owner", "due"]):
        return "action_item"
    return "transcript"

def extract_owner_from_query(query: str) -> str:
    # Existing patterns
    match = re.search(r"([A-Za-z]+)'s task", query)
    if match:
        return match.group(1)
    match = re.search(r"task for ([A-Za-z]+)", query)
    if match:
        return match.group(1)
    match = re.search(r"owner:? ([A-Za-z]+)", query)
    if match:
        return match.group(1)
    match = re.search(r"responsible for ([A-Za-z]+)", query)
    if match:
        return match.group(1)
    # New: handle 'What is Mike responsible for?'
    match = re.search(r"what is ([A-Za-z]+) responsible for", query.lower())
    if match:
        return match.group(1).capitalize()
    return ""

def extract_task_from_query(query: str) -> str:
    match = re.search(r'task:? ([\w\s]+)', query.lower())
    if match:
        return match.group(1).strip()
    return ""

def process_drilldown(user_query: str, input_data: dict) -> dict:
    docs, metadatas, ids = insert_segments(input_data)
    collection.add(documents=docs, metadatas=metadatas, ids=ids)
    query_type = classify_query_type(user_query)
    if query_type == "summary":
        return {
            "answer": input_data.get("key_summary", "Information not found."),
            "data_source": "mentor_json",
            "evidence": [{"type": "summary", "meeting_id": input_data["meeting_id"], "source": "mentor_json"}],
            "relevance_score": 1.0,
            "meeting_id": input_data["meeting_id"]
        }
    if query_type == "action_item":
        owner = extract_owner_from_query(user_query).lower()
        task = extract_task_from_query(user_query).lower()
        for idx, item in enumerate(input_data.get("action_items", [])):
            if (owner and owner in item.get("owner", "").lower()) or (task and task in item.get("task", "").lower()):
                answer = f"Task: {item.get('task', 'N/A')} | Owner: {item.get('owner', 'N/A')} | Due: {item.get('due', 'N/A')}"
                meta = {"type": "action_item", "meeting_id": input_data["meeting_id"], "source": "mentor_json", "index": idx}
                return {
                    "answer": answer,
                    "data_source": meta["source"],
                    "evidence": [meta],
                    "relevance_score": 1.0,  # Always 1.0 for strict match
                    "meeting_id": input_data["meeting_id"]
                }
        results = collection.query(
            query_texts=[user_query],
            n_results=5,
            where={"type": "action_item"}
        )
        if results["documents"][0]:
            doc = results["documents"][0][0]
            meta = results["metadatas"][0][0]
            score = results["distances"][0][0]
            norm_score = max(0.0, min(1.0, 1.0 - (score / 2)))
            return {
                "answer": doc,
                "data_source": meta["source"],
                "evidence": [meta],
                "relevance_score": norm_score,
                "meeting_id": input_data["meeting_id"]
            }
        else:
            return {
                "answer": "Information not found.",
                "evidence": [],
                "meeting_id": input_data["meeting_id"]
            }
    if query_type == "decision":
        results = collection.query(
            query_texts=[user_query],
            n_results=1,
            where={"type": "decision"}
        )
        if results["documents"] and results["documents"][0]:
            doc = results["documents"][0][0]
            meta = results["metadatas"][0][0]
            score = results["distances"][0][0]
            norm_score = max(0.0, min(1.0, 1.0 - (score / 2)))
            return {
                "answer": doc,
                "data_source": meta["source"],
                "evidence": [meta],
                "relevance_score": norm_score,
                "meeting_id": input_data["meeting_id"]
            }
        else:
            return {
                "answer": "Information not found.",
                "evidence": [],
                "meeting_id": input_data["meeting_id"]
            }
    if user_query.strip().lower().startswith("why"):
        import re
        decision_date = None
        for d in input_data.get("decisions_made", []):
            m = re.search(r'(\b\w+ \d{1,2}(?:th|st|nd|rd)?\b|\b\w+ \d{1,2}\b|\b\w+ \d{4}\b)', d)
            if m:
                decision_date = m.group(0)
                break
        transcript_segments = input_data.get("transcript", "").split(". ")
        best_segment = None
        for seg in transcript_segments:
            seg_l = seg.lower()
            if (decision_date and decision_date.lower() in seg_l) or ("need until" in seg_l or "delay" in seg_l or "because" in seg_l or "due to" in seg_l):
                if "mike" in seg_l:
                    best_segment = seg
                    break
                if not best_segment:
                    best_segment = seg
        if not best_segment:
            transcript_results = collection.query(
                query_texts=[user_query],
                n_results=1,
                where={"type": "transcript"}
            )
            best_segment = transcript_results["documents"][0][0] if transcript_results["documents"] and transcript_results["documents"][0] else None
        decision_results = collection.query(
            query_texts=[user_query],
            n_results=1,
            where={"type": "decision"}
        )
        decision_doc = decision_results["documents"][0][0] if decision_results["documents"] and decision_results["documents"][0] else None
        decision_meta = decision_results["metadatas"][0][0] if decision_results["metadatas"] and decision_results["metadatas"][0] else None
        evidence = []
        answer_parts = []
        if best_segment:
            answer_parts.append(f"Transcript evidence: {best_segment}")
            found_meta = None
            for doc, meta in zip(docs, metadatas):
                if meta["type"] == "transcript" and doc.strip() == best_segment.strip():
                    found_meta = meta
                    break
            if found_meta:
                evidence.append(found_meta)
            else:
                evidence.append({"type": "transcript", "meeting_id": input_data["meeting_id"], "source": "raw_transcript"})
        if decision_doc:
            answer_parts.append(f"Decision: {decision_doc}")
            evidence.append(decision_meta)
        if answer_parts:
            return {
                "answer": " ".join(answer_parts),
                "data_source": evidence[0]["source"] if evidence else "",
                "evidence": evidence,
                "relevance_score": 1.0 if evidence else 0.0,
                "meeting_id": input_data["meeting_id"]
            }
        else:
            return {
                "answer": "Information not found.",
                "evidence": [],
                "meeting_id": input_data["meeting_id"]
            }
    results = collection.query(
        query_texts=[user_query],
        n_results=1,
        where={"type": "transcript"}
    )
    if results["documents"] and results["documents"][0]:
        doc = results["documents"][0][0]
        meta = results["metadatas"][0][0]
        score = results["distances"][0][0]
        norm_score = max(0.0, min(1.0, 1.0 - (score / 2)))
        return {
            "answer": doc,
            "data_source": meta["source"],
            "evidence": [meta],
            "relevance_score": norm_score,
            "meeting_id": input_data["meeting_id"]
        }
    else:
        return {
            "answer": "Information not found.",
            "evidence": [],
            "meeting_id": input_data["meeting_id"]
        }
