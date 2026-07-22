import os
import re

def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # The file currently contains: f"Bearer {os.environ.get(\'OLLAMA_API_KEY\')}"
    # with a literal backslash. We want to replace it with:
    # 'Bearer ' + str(os.environ.get('OLLAMA_API_KEY'))
    
    new_content = content.replace(
        r"f'Bearer {os.environ.get(\'OLLAMA_API_KEY\')}'".replace("'", '"').replace('\\"', "\\'"), # Wait, let's just do a simple replace
        r"f'Bearer {os.environ.get(\'OLLAMA_API_KEY\')}'" # No, let's use regex
    )
    
    new_content = re.sub(
        r'f"Bearer \{os\.environ\.get\(\\\'OLLAMA_API_KEY\\\'\)\}"',
        r'f"Bearer {os.environ.get(\"OLLAMA_API_KEY\")}"'.replace('"', "'"),
        content
    )
    # Actually, simpler:
    new_content = content.replace(
        'f"Bearer {os.environ.get(\\\'OLLAMA_API_KEY\\\')}"',
        'f\'Bearer {os.environ.get("OLLAMA_API_KEY")}\''
    )
    # Let's just replace the whole headers part:
    new_content = content.replace(
        'headers={"Authorization": f"Bearer {os.environ.get(\\\'OLLAMA_API_KEY\\\')}"}',
        'headers={"Authorization": f\'Bearer {os.environ.get("OLLAMA_API_KEY")}\'}'
    )
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f'Fixed {filepath}')

for root, _, files in os.walk('engine'):
    for file in files:
        if file.endswith('.py'):
            patch_file(os.path.join(root, file))
