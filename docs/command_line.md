## Virtual environement

### Delete broken .conda folder

Remove-Item -Recurse -Force .\.conda

### (Re)create it properly with venv

python -m venv .conda

### Activate scripts

.\.conda\Scripts\activate



## Requirements

pip freeze > requirements.txt
