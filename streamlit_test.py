import streamlit as st

st.title("File Upload Example")

uploaded_file = st.file_uploader("Choose a file")

if uploaded_file is not None:
    # Read file as bytes
    content = uploaded_file.read()

    st.subheader("File details")
    st.write("Filename:", uploaded_file.name)
    st.write("File size:", len(content), "bytes")

    st.subheader("File contents")
    try:
        # Try to decode as text
        text = content.decode("utf-8")
        st.text(text)
    except UnicodeDecodeError:
        st.warning("This file is not a text file.")
