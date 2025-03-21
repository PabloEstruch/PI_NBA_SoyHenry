@echo off

echo Activando entorno virtual...
"C:\Users\estefania_noguerab\Documents\ProyectoFinal\mi_entorno\Scripts\activate"
pause

echo Instalando dependencias de requirements.txt...
pip install -r "C:\Users\estefania_noguerab\Documents\ProyectoFinal\requirements.txt"
pause

echo Ejecutando download.py...
python "C:\Users\estefania_noguerab\Documents\ProyectoFinal\download.py"
pause

echo Ejecutando pbp.py...
python "C:\Users\estefania_noguerab\Documents\ProyectoFinal\pbp.py"
pause

echo Proceso completado.
pause