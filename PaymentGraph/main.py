import streamlit as st
import json
from dotenv import load_dotenv
from langchain_neo4j import Neo4jGraph
from groq import Groq

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

# -----------------------------
# EXTRACT TX ID
# -----------------------------
def extract_tx_id(record):
    try:
        return record["message"]["logger"]["transaction_id"]
    except:
        return None


# -----------------------------
# CLEAR DATABASE (IMPORTANT)
# -----------------------------
def clear_database(graph):
    graph.query("MATCH (n) DETACH DELETE n")


# -----------------------------
# STORE DATA WITH CORRECT GRAPH
# -----------------------------
def store_transaction(graph, record):

    tx_id = extract_tx_id(record)
    if not tx_id:
        return

    msg = record["message"]["logger"]

    payment = msg.get("payment", {})
    accounts = msg.get("accounts", {})
    fmt = msg.get("format", {})
    details = fmt.get("details", {})

    query = """
    MERGE (t:Transaction {id: $tx_id})

    MERGE (p:Payment {
        amount: $amount,
        currency: $currency,
        status: $status
    })
    MERGE (t)-[:HAS_PAYMENT]->(p)

    MERGE (s:Account {iban: $sender})
    MERGE (t)-[:SENT_BY]->(s)

    MERGE (r:Account {iban: $receiver})
    MERGE (t)-[:RECEIVED_BY]->(r)

    MERGE (f:Format {
        type: $type,
        version: $version
    })
    MERGE (t)-[:USES_FORMAT]->(f)

    MERGE (d:Details {
        channel: $channel,
        priority: $priority
    })
    MERGE (t)-[:HAS_DETAILS]->(d)
    """

    graph.query(query, {
        "tx_id": tx_id,
        "amount": payment.get("amount"),
        "currency": payment.get("currency"),
        "status": payment.get("status"),
        "sender": accounts.get("sender", {}).get("IBAN"),
        "receiver": accounts.get("receiver", {}).get("IBAN"),
        "type": fmt.get("type"),
        "version": fmt.get("version"),
        "channel": details.get("channel"),
        "priority": details.get("priority")
    })


# -----------------------------
# PROCESS JSON
# -----------------------------
def process_files(graph, files):

    clear_database(graph)   # 🔥 IMPORTANT RESET

    count = 0

    for file in files:
        data = json.load(file)

        for record in data:
            if extract_tx_id(record):
                store_transaction(graph, record)
                count += 1

    st.success(f"Inserted {count} transactions ✅")


# -----------------------------
# MODEL SELECT
# -----------------------------
def get_model(client):
    models = client.models.list()
    for m in models.data:
        if "llama" in m.id.lower():
            return m.id
    return models.data[0].id


# -----------------------------
# PROMPT
# -----------------------------
def build_prompt(question):
    return f"""
You are a Neo4j expert.

STRICT RULES:
- Return ONLY Cypher query
- Do NOT explain
- Do NOT return anything else

GRAPH:

(Transaction)-[:HAS_PAYMENT]->(Payment)
(Transaction)-[:SENT_BY]->(Account)
(Transaction)-[:RECEIVED_BY]->(Account)
(Transaction)-[:USES_FORMAT]->(Format)
(Transaction)-[:HAS_DETAILS]->(Details)

Account: iban
Payment: amount, currency, status
Format: type, version
Details: channel, priority

QUESTION:
{question}
"""


# -----------------------------
# VALIDATE CYPHER
# -----------------------------
def is_valid_cypher(q):
    if not q:
        return False
    return q.strip().upper().startswith(("MATCH", "RETURN", "WITH"))


# -----------------------------
# LLM CALL
# -----------------------------
def ask_llm(prompt, api_key):

    client = Groq(api_key=api_key)
    model = get_model(client)

    res = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )

    output = res.choices[0].message.content.strip()

    if "```" in output:
        output = output.split("```")[1]

    return output.strip()


# -----------------------------
# SAFE CYPHER GENERATION
# -----------------------------
def generate_cypher(prompt, api_key):

    cypher = ask_llm(prompt, api_key)

    if is_valid_cypher(cypher):
        return cypher

    # retry once
    cypher = ask_llm(prompt + "\nONLY CYPHER QUERY.", api_key)

    return cypher


# -----------------------------
# NATURAL LANGUAGE ANSWER
# -----------------------------
def format_answer(result, question, api_key):

    if not result:
        return "No matching data found in the database."

    client = Groq(api_key=api_key)
    model = get_model(client)

    prompt = f"""
Convert this result into a simple natural sentence.

Question: {question}
Result: {result}

Give a clean answer.
"""

    res = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )

    return res.choices[0].message.content.strip()


# -----------------------------
# STREAMLIT UI
# -----------------------------
def main():

    st.title("💳 GraphRAG Financial System (FINAL)")

    api_key = st.sidebar.text_input("Groq API Key", type="password")
    url = st.sidebar.text_input("Neo4j URL", "bolt://127.0.0.1:7687")
    user = st.sidebar.text_input("Username", "neo4j")
    pwd = st.sidebar.text_input("Password", type="password")

    if "graph" not in st.session_state:
        st.session_state.graph = None

    if st.sidebar.button("Connect"):
        st.session_state.graph = Neo4jGraph(url=url, username=user, password=pwd)
        st.success("Connected ✅")

    if st.session_state.graph and api_key:

        files = st.file_uploader("Upload JSON", type="json", accept_multiple_files=True)

        if files and st.button("Process Data"):
            process_files(st.session_state.graph, files)

        st.subheader("Ask Question")
        question = st.text_input("Enter your query")

        if st.button("Ask"):

            prompt = build_prompt(question)

            cypher = generate_cypher(prompt, api_key)

            st.code(cypher, language="cypher")

            if not is_valid_cypher(cypher):
                st.error("Invalid query generated")
                return

            result = st.session_state.graph.query(cypher)

            st.write("Raw Result:", result)

            answer = format_answer(result, question, api_key)

            st.success(answer)


if __name__ == "__main__":
    main()