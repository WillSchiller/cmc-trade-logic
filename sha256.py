import hashlib

def sha256_hash(text):
    # Encode the text to bytes
    text_bytes = text.encode()

    # Create a SHA256 hash object
    sha256 = hashlib.sha256()

    # Update the hash object with the bytes
    sha256.update(text_bytes)

    # Get the hexadecimal representation of the hash
    hex_digest = sha256.hexdigest()

    return hex_digest