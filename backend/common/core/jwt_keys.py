"""
Менеджер асимметричных ключей (RSA / ECDSA) для подписи JWT-токенов,
ротации ключей и экспорта набора публичных ключей JWKS (RFC 7517).
"""

import base64
import hashlib
import time
from typing import Any, Dict, List, Optional
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import jwt


def _int_to_base64url(val: int) -> str:
    """Преобразует целое число в формат Base64URL без паддинга."""
    byte_len = (val.bit_length() + 7) // 8
    val_bytes = val.to_bytes(byte_len, byteorder="big")
    return base64.urlsafe_b64encode(val_bytes).decode("utf-8").rstrip("=")


class JWTKeyManager:
    """
    Менеджер ключей подписи JWT токенов.
    Поддерживает:
    - Асимметричные алгоритмы (RS256, ES256).
    - Ротацию ключей с сохранением истории публичных ключей для валидации ранее выпущенных токенов.
    - Экспорт публичных ключей в формате JWKS (JSON Web Key Set).
    """

    def __init__(self, default_algorithm: str = "RS256"):
        self.default_algorithm = default_algorithm
        self._private_keys: Dict[str, Any] = {}
        self._public_keys: Dict[str, Any] = {}
        self._active_kid: Optional[str] = None

    def generate_rsa_key_pair(self, key_size: int = 2048, kid: Optional[str] = None) -> str:
        """Генерирует новую RSA-пару ключей и делает её активной для подписи."""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
        )
        public_key = private_key.public_key()

        if not kid:
            der_bytes = public_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            kid = f"rsa-{hashlib.sha256(der_bytes).hexdigest()[:12]}-{int(time.time())}"

        self._private_keys[kid] = private_key
        self._public_keys[kid] = public_key
        self._active_kid = kid
        return kid

    def load_rsa_private_key_pem(self, pem_bytes: bytes, kid: Optional[str] = None) -> str:
        """Загружает приватный ключ RSA из PEM-формата."""
        private_key = serialization.load_pem_private_key(pem_bytes, password=None)
        public_key = private_key.public_key()

        if not kid:
            der_bytes = public_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            kid = f"rsa-{hashlib.sha256(der_bytes).hexdigest()[:12]}"

        self._private_keys[kid] = private_key
        self._public_keys[kid] = public_key
        self._active_kid = kid
        return kid

    def get_active_kid(self) -> Optional[str]:
        return self._active_kid

    def get_active_private_key(self) -> Optional[Any]:
        if self._active_kid and self._active_kid in self._private_keys:
            return self._private_keys[self._active_kid]
        return None

    def get_public_key(self, kid: str) -> Optional[Any]:
        return self._public_keys.get(kid)

    def get_jwks(self) -> Dict[str, List[Dict[str, Any]]]:
        """Формирует JSON Web Key Set (JWKS) со всеми известными публичными ключами."""
        keys = []
        for kid, pub_key in self._public_keys.items():
            if isinstance(pub_key, rsa.RSAPublicKey):
                numbers = pub_key.public_numbers()
                keys.append(
                    {
                        "kty": "RSA",
                        "use": "sig",
                        "alg": "RS256",
                        "kid": kid,
                        "n": _int_to_base64url(numbers.n),
                        "e": _int_to_base64url(numbers.e),
                    }
                )
        return {"keys": keys}

    def sign_jwt(self, payload: Dict[str, Any]) -> str:
        """Подписывает токен активным асимметричным ключом с добавлением kid в заголовок JWT."""
        if not self._active_kid or self._active_kid not in self._private_keys:
            raise ValueError("No active asymmetric private key available in KeyManager")

        private_key = self._private_keys[self._active_kid]
        headers = {"kid": self._active_kid}
        return jwt.encode(payload, private_key, algorithm=self.default_algorithm, headers=headers)

    def verify_jwt(self, token: str) -> Optional[Dict[str, Any]]:
        """Проверяет токен, сопоставляя kid из заголовка с набором известных публичных ключей."""
        try:
            unverified_headers = jwt.get_unverified_header(token)
            kid = unverified_headers.get("kid")
            if not kid or kid not in self._public_keys:
                if self._active_kid and self._active_kid in self._public_keys:
                    pub_key = self._public_keys[self._active_kid]
                else:
                    return None
            else:
                pub_key = self._public_keys[kid]

            return jwt.decode(token, pub_key, algorithms=[self.default_algorithm])
        except (jwt.PyJWTError, KeyError, ValueError):
            return None


# Глобальный инстанс менеджера ключей платформы
global_jwt_key_manager = JWTKeyManager()
