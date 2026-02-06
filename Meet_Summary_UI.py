import streamlit as st
from pypdf import PdfReader
import tempfile
import os

def extract_text_from_pdf(pdf_path):
    # Initialize the reader
    reader = PdfReader(pdf_path)
    
    # Get total number of pages
    number_of_pages = len(reader.pages)
    print(f"Reading {number_of_pages} pages...")

    full_text = ""
    
    # Loop through each page and extract text
    for i in range(number_of_pages):
        page = reader.pages[i]
        full_text += page.extract_text() + "\n"
        
    return full_text


# Set page title
st.title("📁 Meeting Alchemist")

# 1. Sidebar selection for different modes
choice = st.sidebar.radio(
    "Select Input Method:",
    ("Text/PDF File Upload", "Plain Text Upload","Audio File Upload", "Live Recording", "API Connectivity")
)

st.header(f"Mode: {choice}")

# a) Text/PDF File Upload
if choice == "Text/PDF File Upload":
    uploaded_text = st.file_uploader("Choose a text or PDF file", type=["txt", "pdf"])
    if uploaded_text:
        st.success(f"Loaded: {uploaded_text.name}")
        # Save uploaded file to a temporary file to get absolute path
        suffix = os.path.splitext(uploaded_text.name)[-1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file.write(uploaded_text.read())
            abs_path = tmp_file.name
        st.info(f"Temporary file saved at: {abs_path}")
        # For .txt files, read and display content
        if uploaded_text.name.endswith(".txt"):
            with open(abs_path, 'r', encoding='utf-8') as f:
                content = f.read()
            st.write("**File Content:**")
            st.text(content)
        # For .pdf files, use your extract_text_from_pdf function
        elif uploaded_text.name.endswith(".pdf"):
            file_content = extract_text_from_pdf(abs_path)
            st.text_area("File Content Preview:", file_content, height=200)
            st.success("File read successfully!") 

# b) Audio File Upload
elif choice == "Audio File Upload":
    uploaded_audio = st.file_uploader("Upload an audio file", type=["mp3", "wav", "m4a"])
    if uploaded_audio:
        suffix = os.path.splitext(uploaded_audio.name)[-1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file.write(uploaded_audio.read())
            abs_audio_path = tmp_file.name
        st.info(f"Temporary audio file saved at: {abs_audio_path}")
        st.audio(abs_audio_path)
        st.success("Audio file ready for processing.")

# c) Recording Option (Using Streamlit's built-in audio_input)
elif choice == "Live Recording":
    st.write("Click below to record your voice:")
    audio_data = st.audio_input("Record a message")
    if audio_data:
        suffix = ".wav"  # Streamlit audio_input returns wav format
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file.write(audio_data.getvalue())
            abs_record_path = tmp_file.name
        st.info(f"Temporary recording saved at: {abs_record_path}")
        st.audio(abs_record_path)
        st.success("Recording captured!")

# d) API Connectivity
elif choice == "API Connectivity":
    with st.form("api_form"):
        api_url = st.text_input("API Endpoint URL", placeholder="https://api.example.com/v1/data")
        api_key = st.text_input("API Key / Bearer Token", type="password")
        submit = st.form_submit_button("Test Connection")
        
        if submit:
            if api_url and api_key:
                st.spinner("Testing connectivity...")
                st.info(f"Attempting to connect to {api_url}")
            else:
                st.error("Please provide both URL and API Key.")

# e) Plain Text Upload
elif choice == "Plain Text Upload":
    user_text = st.text_area("Enter your message:", height=250)
    if user_text:
        st.write(f"You entered: {user_text}")

# Add Submit and Cancel buttons at the bottom
col1, col2 = st.columns(2)
with col1:
    submit_btn = st.button("Submit")
with col2:
    cancel_btn = st.button("Cancel")

if submit_btn:
    st.success("Submission successful!")
if cancel_btn:
    st.warning("Submission cancelled.")