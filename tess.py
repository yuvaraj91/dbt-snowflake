import base64

def encoding(x):
    x = x.encode("ascii")
    base64_bytes = base64.b64encode(x)
    return base64_bytes.decode("ascii")
xx='117 east 57th street'
print(xx)
print(encoding(xx))