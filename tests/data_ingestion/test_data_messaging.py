from microquake.core import read
from io import BytesIO
from base64 import b64decode, b64encode

# reading the data

st = read('../../data/20170419_153133.mseed', format='MSEED')

# encoding option 1

buf = BytesIO()
st.write(buf, format='MSEED')
st_encoded = (buf.getvalue())
key = st[0].stats.starttime
message = (key, st_encoded)

# decoding option 1

buf = BytesIO(message[1])
st_decoded1 = read(buf, format='MSEED')

# encoding option 2

st_encoded = b64encode(buf.getvalue())
message = (key, st_encoded)

# decoding option 2

buf = BytesIO(b64decode(message[1]))
st_decoded2 = read(buf, format='MSEED')

