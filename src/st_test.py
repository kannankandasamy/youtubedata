import streamlit as st
import pandas as pd
import altair as alt
from PIL import Image

# initialize list of lists
data = [['Le goumet', 10], ['The Alcove', 15], ['Mojo Restaurant', 14], ['Mojo Restaurant', 1]]

# Create the pandas DataFrame
df = pd.DataFrame(data, columns=['Name', 'ID'])

values = df['Name'].tolist()
options = df['ID'].tolist()
dic = dict(zip(options, values))

a = st.sidebar.selectbox('Choose a restaurant', options, format_func=lambda x: dic[x])


image = Image.open('images/arch.drawio.png')
st.image(image, caption="Architecture")