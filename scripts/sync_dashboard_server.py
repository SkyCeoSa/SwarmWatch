with open('dashboard.html', 'r', encoding='utf-8') as f:
    dashboard_content = f.read()

with open('packages/sdk-python/swarmwatch/server.py', 'r', encoding='utf-8') as f:
    server_content = f.read()

q3 = '"""'
start_marker = 'DASHBOARD_HTML = ' + q3
start_idx = server_content.find(start_marker)
assert start_idx != -1, 'Start marker not found'
content_start = start_idx + len(start_marker)

end_idx = server_content.find(q3, content_start + 100)
assert end_idx != -1, 'End marker not found'

new_server = server_content[:content_start] + dashboard_content + server_content[end_idx:]
with open('packages/sdk-python/swarmwatch/server.py', 'w', encoding='utf-8') as f:
    f.write(new_server)

print('server.py DASHBOARD_HTML synchronized successfully!')
