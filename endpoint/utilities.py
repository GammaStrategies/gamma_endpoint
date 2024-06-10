from fastapi import Response


def add_deprecated_message(
    response: Response, message: str = "This endpoint is no longer valid"
) -> Response:
    """Modify the response to include a deprecated message in the header ( X-Deprecated ) and status code 299

    Args:
        response (Response):
        message (str, optional): message to show in X-Deprecated header key. Defaults to "This endpoint is no longer valid".

    Returns:
        Response: modified response
    """

    response.status_code = 299
    response.headers["X-Deprecated"] = message
    return response
