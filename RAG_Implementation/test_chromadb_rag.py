import json
from rag_module import process_drilldown

input_data = {
    "meeting_id": "MGT-101",
    "transcript": "Sarah: We should aim for a March launch. Mike: I need until April 15th for the database.",
    "key_summary": "The team discussed shifting the Q2 launch timeline.",
    "decisions_made": [
        "The launch date is officially moved to April 15th."
    ],
    "action_items": [
        {
            "task": "Update the roadmap",
            "owner": "Sarah",
            "due": "Feb 20th"
        },
        {
            "task": "Complete database migration",
            "owner": "Mike",
            "due": "April 1st"
        }
    ]
}

# Example queries to test ChromaDB-backed RAG
queries = [
    "Why was the launch delayed?",
    "What is Mike responsible for?",
    "When is Sarah's task due?",
    "What was the meeting about?"
]

for user_query in queries:
    print(f"\nQuery: {user_query}")
    result = process_drilldown(user_query, input_data)
    print(json.dumps(result, indent=2))
