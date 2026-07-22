import os
import re

def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = content.replace(
        'f"Bearer {os.environ.get(\'OLLAMA_API_KEY\')}"',
        '"Bearer " + str(os.environ.get("OLLAMA_API_KEY", ""))'
    )
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f'Fixed {filepath}')

for root, _, files in os.walk('engine'):
    for file in files:
        if file.endswith('.py'):
            patch_file(os.path.join(root, file))
