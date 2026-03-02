import re


def safe_name(value: str) -> str:
    return "".join(c for c in value if c.isalnum() or c in " -_").strip()


def normalize_xml_string(xml_string: str) -> str:
    lines = xml_string.splitlines()
    lines = [line.strip() for line in lines if line.strip()]
    lines = " ".join(lines)
    return f"{lines}"


def replace_string(value: str, old_name: str, new_name: str) -> str:
    if value is None:
        return value
    value = value.replace(f"[{old_name}]", f"[{new_name}]")
    value = re.sub(rf"(?<![\w\.]){re.escape(old_name)}(?![\w\.])", new_name, value)
    return value
