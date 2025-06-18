import re

def topic_url_to_name(topic_url: str) -> str:
    # 1. strip slashes
    clean = topic_url.strip('/')
    if not clean:
        return ''

    # 2. grab only the last segment
    segment = clean.split('/')[-1]

    # 3. split into parts on '-' or '_'
    parts = re.split(r'[-_]+', segment)

    # 4. if there's more than one part, drop the last one
    if len(parts) > 1:
        parts = parts[:-1]

    # 5. join with underscores, lowercase
    return '_'.join(parts).lower()

