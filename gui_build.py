python -m nuitka --standalone --follow-imports --enable-plugin=tk-inter --disable-console gui3.py
pyinstaller --onedir --hidden-import openpyxl.cell._writer gui3.py

Used pyinstaller here: Use python 3.11 env: gui2; use cmd cpromt python (nott conda) >> set pythonpath and pythonhome:
check: where python
setx PYTHONPATH "C:\path\to\python directory"
setx PYTHOHOME "C:\path\to\python directory"

