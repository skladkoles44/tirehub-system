class FourTochkiError(Exception):
    """Base integration error."""


class FourTochkiTransportError(FourTochkiError):
    """Transport / SOAP connectivity / endpoint errors."""


class FourTochkiAuthError(FourTochkiError):
    """Authentication rejected or invalid credentials."""


class FourTochkiSemanticError(FourTochkiError):
    """Runtime semantic mismatch versus frozen baseline."""


class FourTochkiDataError(FourTochkiError):
    """Malformed or unexpected response payload."""
