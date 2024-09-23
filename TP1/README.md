[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/_Z0Xw1Zc)
Instrucciones
-------------

- Descargar el dataset de https://www.kaggle.com/datasets/skihikingkevin/pubg-match-deaths 
- Descomprimir y guardar los contenidos de la carpeta `deaths` en un path conocido.
- Implementar el código según el enunciado https://concurrentes-fiuba.github.io/2024_2C_tp1.html

Ejecución
---------

```
cargo run <input-path> <num-threads> <output-file-name>
```

por ejemplo

```
cargo run ~/Downloads/dataset/deaths 4 output.json
```

Pruebas
-------

- La salida de la ejecución con el dataset completo debe ser igual a la del archivo `expected_output.json`, sin importar
  el orden de aparición de las keys en los mapas.