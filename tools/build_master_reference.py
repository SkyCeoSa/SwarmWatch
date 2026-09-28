import re
import os

v1_path = r'docs/research_iran/IRAN_STARTUP_DEEP_RESEARCH_VAULT.md'
v2_path = r'docs/research_iran/IRAN_STARTUP_DEEP_RESEARCH_VAULT_PART_2.md'
v3_path = r'docs/research_iran/IRAN_FUTURE_AND_LABOR_MARKET_VAULT.md'
out_path = r'docs/research_iran/IRAN_STARTUP_MASTER_REFERENCE_ALL_180_CYCLES.md'

with open(v1_path, 'r', encoding='utf-8') as f:
    v1_content = f.read()

with open(v2_path, 'r', encoding='utf-8') as f:
    v2_content = f.read()

with open(v3_path, 'r', encoding='utf-8') as f:
    v3_content = f.read()

# Build master TOC
all_contents = [
    ('بخش اول: مجلد اول (لوپ‌های ۱ تا ۳ — چرخه‌های ۱ تا ۶۰)', v1_content),
    ('بخش دوم: مجلد دوم (لوپ‌های ۴ تا ۶ — چرخه‌های ۶۱ تا ۱۲۰)', v2_content),
    ('بخش سوم: مجلد سوم (لوپ‌های ۷ تا ۹ — چرخه‌های ۱۲۱ تا ۱۸۰)', v3_content)
]

toc_lines = ['## 📑 فهرست تفصیلی دسترسی به ۱۸۰ ایده استارتاپی در ۹ لوپ تخصصی\n']

for vname, c in all_contents:
    toc_lines.append(f'### {vname}\n')
    matches = re.findall(r'(###\s+چرخه\s+(\d+)[:\s]+([^\n]+))', c)
    for m in matches:
        c_num = int(m[1])
        c_title = m[2].strip()
        toc_lines.append(f'- **چرخه {c_num}:** {c_title}')
    toc_lines.append('\n')

toc_text = '\n'.join(toc_lines)

master_header = '''# 🇮🇷 دانشنامه مرجع و جامع ۱۸۰ ایده استارتاپی بازار ایران (Iran Startup Master Compendium)
## کلیه استدلال‌ها، کالبدشکافی مسلخ وکیل مدافع شیطان، شناسنامه فنی و مدل اقتصادی ۹۰۰ ایده خام در ۱۸۰ چرخه کامل
### (سه‌گانه جامع: مجلدات ۱، ۲ و ۳ در قالب یک سند مرجع یکپارچه)

> [!IMPORTANT]
> **مشخصات سند مرجع:**
> - **تعداد کل چرخه‌ها:** ۱۸۰ چرخه عمیق متوالی در ۹ لوپ استراتژیک تخصصی
> - **تعداد کل ایده‌های خام ورودی:** ۹۰۰ ایده استارتاپی
> - **تعداد ایده‌های مسلخ‌شده توسط وکیل مدافع شیطان:** ۷۲۰ ایده (با ذکر دقیق دلایل رد در ۴ گیت)
> - **تعداد قهرمانان بازمانده با شناسنامه کامل:** ۱۸۰ استارتاپ نرم‌افزاری مستقل
> - **میانگین حاشیه سود ناخالص:** ۹۳.۲٪ (حاشیه سود خالص نرم‌افزاری)
> - **قابلیت اجرا صفر تا صد توسط AI Agent:** ۱۰۰٪ بدون نیاز به حضور فیزیکی، دفتر، انبار یا مجوزهای انحصاری
> - **پوشش موضوعی:** تجارت الکترونیک، فین‌تک، مالیات، زیرساخت وب، اقتصاد سایه و رمزارز، بحران آب و انرژی، اگروتک، سلامت تخصصی و دندانپزشکی، سالمندی، بازار کار و روابط کارگری، مهاجرت اقلیمی و زیست‌بوم شمال، و آینده اقتصاد کلان و آنتی‌فراژیل.

---
'''

master_doc = master_header + '\n' + toc_text + '\n---\n\n'
master_doc += '# 🏛️ بخش نخست: مجلد اول (لوپ‌های ۱ تا ۳ — چرخه‌های ۱ تا ۶۰)\n\n' + v1_content + '\n\n---\n\n'
master_doc += '# 🏛️ بخش دوم: مجلد دوم (لوپ‌های ۴ تا ۶ — چرخه‌های ۶۱ تا ۱۲۰)\n\n' + v2_content + '\n\n---\n\n'
master_doc += '# 🏛️ بخش سوم: مجلد سوم (لوپ‌های ۷ تا ۹ — چرخه‌های ۱۲۱ تا ۱۸۰)\n\n' + v3_content + '\n'

with open(out_path, 'w', encoding='utf-8') as f:
    f.write(master_doc)

print('SUCCESS')
print('Output Path:', os.path.abspath(out_path))
print('Total Bytes:', len(master_doc.encode('utf-8')))
