# RAG Product Manual

## Overview
This Retrieval-Augmented Generation (RAG) system is designed for meeting data, using only local models and ChromaDB for retrieval. It supports evidence traceability and returns the highest-scoring evidence for a user query. All input/output is JSON (Python dicts), and the core logic is UI-agnostic.

## Key Features
- **Local Embedding Model:** Uses a local model (gte-large) or a fixed-length hash fallback for embeddings.
- **ChromaDB Integration:** All meeting segments (action items, decisions, transcript) are stored as documents with metadata and unique IDs.
- **Evidence Traceability:** Each answer includes the source and metadata for the evidence.
- **Query Routing:** Queries are classified as summary, decision, action item, or transcript, and routed to the correct data bucket.
- **Causal/Why Queries:** For 'why' questions, the system synthesizes an answer from both the most relevant transcript and decision.
- **Robust Fallbacks:** If no semantic match is found, the system falls back to strict owner/task matching for action items or returns a clear not found message.

## Usage
- Place your local embedding model in `C:\Models\gte-large` or update the path in `rag_module.py`.
- Install dependencies from `requirements.txt`.
- Run `test_chromadb_rag.py` to test the system with sample meeting data and queries.

## File Structure
- `rag_module.py`: Core RAG logic and ChromaDB integration.
- `test_chromadb_rag.py`: Test script for ChromaDB-backed RAG.
- `requirements.txt`: Python dependencies.
- `PRODUCT_MANUAL.md`: This manual.

## Example Query/Response
```
Query: Why was the launch delayed?
{
  "answer": "Transcript evidence: Mike: I need until April 15th for the database. Decision: The launch date is officially moved to April 15th.",
  "data_source": "raw_transcript",
  "evidence": [ ... ],
  "relevance_score": 1.0,
  "meeting_id": "MGT-101"
}
```

## Troubleshooting
- If you see errors about embedding dimensions, ensure the fallback embedding returns a fixed-length, normalized vector.
- If ChromaDB tries to download a model, ensure you pass the local embedding function as shown in `rag_module.py`.

## Customization
- You can extend query classification or add more advanced filtering in `rag_module.py`.
- For batch uploads or advanced analytics, add new functions to the module as needed.
