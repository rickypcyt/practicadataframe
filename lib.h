// fichero cabecera con las declaraciones de tipos y prototipos de funciones 
//principales de la práctica. Todo el código que se ha escrito en este enunciado 
//(páginas 2 y 3) debe incluirse en este fichero.
// lib.h actúa como un puente entre main.c y lib.c, ya que declara las funciones que main.c utilizará.
#ifndef LIB_H
#define LIB_H

#include <time.h>

// Tipo enumerado para representar los diferentes tipos de datos en las columnas
typedef enum {
    TEXTO,
    NUMERICO,
    FECHA
} TipoDato;

// Estructura para representar una columna del dataframe
typedef struct {
    char nombre[30];
    TipoDato tipo;
    void *datos;
    unsigned char *esNulo;
    int numFilas;
} Columna;

// Estructura para representar el dataframe como un conjunto de columnas
typedef struct {
    Columna *columnas;
    int numColumnas;
    int numFilas;
    int *indice;
} Dataframe;

// Alias para tipo FECHA
typedef struct tm Fecha;

// Estructura para la lista de dataframes
typedef struct NodoLista {
    Dataframe *df;
    struct NodoLista *siguiente;
} Nodo;

typedef struct {
    int numDFs;
    Nodo *primero;
} Lista;

// Declaración de las funciones
void leerCSV(const char *nombreArchivo);
void solicitarNombreArchivo(char *nombreArchivo, size_t size);
void comandos(void);

#endif // LIB_H


