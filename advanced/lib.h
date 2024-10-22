#ifndef LIB_H
#define LIB_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Tipo enumerado para representar los diferentes tipos de datos en las columnas
typedef enum {
    TEXTO,
    NUMERICO,
    FECHA
} TipoDato;

// Estructura para representar una columna del dataframe
typedef struct {
    char nombre[30];      // Nombre de la columna
    TipoDato tipo;        // Tipo de datos de la columna (TEXTO, NUMERICO, FECHA)
    void *datos;          // Puntero genérico para almacenar los datos de la columna
    unsigned char *esNulo; // Array paralelo, indica valores nulos (1/0: nulo/noNulo)
    int numFilas;         // Número de filas en la columna
} Columna;

// Estructura para representar el dataframe como un conjunto de columnas
typedef struct {
    Columna *columnas;    // Array de columnas (con tipos de datos distintos)
    int numColumnas;      // Número de columnas en el dataframe
    int numFilas;         // Número de filas (igual para todas las columnas)
    int *indice;          // Array para ordenar las filas
} Dataframe;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista {
    Dataframe *df;            // Puntero a un dataframe
    struct NodoLista *siguiente; // Puntero al siguiente nodo de la lista
} Nodo;

// Estructura para representar la lista de Dataframe's
typedef struct {
    int numDFs;           // Número de dataframes almacenados en la lista
    Nodo *primero;        // Puntero al primer Nodo de la lista
} Lista;

// Prototipos de funciones
Columna* crearColumna(const char* nombre, TipoDato tipo, int numFilas);
void eliminarColumna(Columna* columna);
Dataframe* crearDataframe();
void eliminarDataframe(Dataframe* df);
Columna* buscarColumna(Dataframe* df, const char* nombreColumna);
void agregarDataframeALista(Lista* lista, Dataframe* df);
void eliminarDataframeDeLista(Lista* lista, Dataframe* df);
void eliminarListaDataframes(Lista* lista);
Dataframe* buscarDataframeEnLista(Lista* lista, const char* nombreDF);
Dataframe* cargarCSV(const char* nombreArchivo);
void guardarCSV(Dataframe* df, const char* nombreArchivo);

#endif // LIB_H
