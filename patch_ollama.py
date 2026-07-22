import os
import re

def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = re.sub(
        r'(f"\{OLLAMA_BASE_URL\}",)',
        r'\1 headers={"Authorization": f"Bearer {os.environ.get(\'OLLAMA_API_KEY\')}"} if os.environ.get("OLLAMA_API_KEY") else {},',
        content
    )
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f'Patched {filepath}')

for root, _, files in os.walk('engine'):
    for file in files:
        if file.endswith('.py'):
            patch_file(os.path.join(root, file))
