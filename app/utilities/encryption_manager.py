import base64
import string
import numpy as np
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


class EncryptionManager:

    block_size = AES.block_size
    punctuation = "!#$%&()*+,-.:;<=>?@[]^_{|}"

    def encrypt(self, base_key, salt, name, original_value):
        n = len(name)
        if n % 2 > 0:
            n += 1
        random_n_chars = "".join(
            np.random.choice(
                list(string.ascii_letters + string.digits + self.punctuation), n
            )
        )

        vector = "".join(
            np.random.choice(
                list(string.ascii_letters + string.digits + self.punctuation), 16
            )
        )

        private_key = PBKDF2(base_key, salt.encode(), dkLen=32)
        message = pad(original_value.encode(), self.block_size)
        cipher = AES.new(private_key, AES.MODE_CBC, iv=vector.encode())
        ct_bytes = cipher.encrypt(message)
        encrypted_text = base64.b64encode(ct_bytes).decode('utf-8')
        encrypted_text_to_store = encrypted_text + vector + random_n_chars
        return encrypted_text_to_store

    def decrypt(self, base_key, salt, name, encrypted_text):
        n = len(name)
        if n % 2 > 0:
            n += 1

        private_key = PBKDF2(base_key, salt.encode(), dkLen=32)
        vector = encrypted_text[-(16+n):-n]
        cipher = AES.new(private_key, AES.MODE_CBC, iv=vector.encode())
        value_to_be_decrypted = base64.b64decode(encrypted_text[:-(16+n)])
        decrypted_text = unpad(cipher.decrypt(value_to_be_decrypted), self.block_size)

        return decrypted_text.decode()
