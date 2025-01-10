// lib.h
#ifndef LIB_H
#define LIB_H

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Constantes de configuración del sistema
#define MAX_LINE_LENGTH 4096
#define MAX_COLUMNS 100
#define MAX_ROWS 5000
#define MAX_FILENAME 256

// Códigos de color ANSI para salida por consola
#define RED "\x1b[31m"
#define GREEN "\x1b[32m"
#define WHITE "\x1b[37m"
#define RESET "\x1b[0m"

// Enumeración de tipos de datos soportados
typedef enum {
  TEXTO,    // Tipo cadena de texto
  NUMERICO, // Tipo numérico (punto flotante)
  FECHA     // Tipo fecha
} TipoDato;

// Estructura de Columna: Representa una columna en el df
typedef struct {
  char nombre[50];      // Nombre de la columna
  TipoDato tipo;        // Tipo de datos de la columna
  char **datos;         // Arreglo de punteros a datos
  unsigned int *esNulo; // Indicador de valores nulos
  int numFilas;         // Número de filas en la columna
} Columna;

// Estructura de df: Contenedor principal de datos
typedef struct {
  Columna *columnas; // Arreglo de columnas
  int numColumnas;   // Número total de columnas
  int numFilas;      // Número total de filas
  char indice[20];   // Nombre del df
} Dataframe;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista {
  Dataframe *df;               // puntero a Dataframe
  struct NodoLista *siguiente; // Puntero al siguiente nodo de la lista
} Nodo;

// Estructura para representar la lista de Dataframe’s
typedef struct {
  int numDFs;    // Número de Dataframes almacenados en la lista
  Nodo *primero; // Puntero al primer Nodo de la lista
} Lista;

// Variables globales para gestión del sistema
extern Lista listaDF;               // Declaración de la variable global
extern Dataframe *df_actual;        // Declaración de la variable global
extern char promptTerminal[MAX_LINE_LENGTH]; // Declaración de la variable global


// Prototipos de todas las funciones utilizadas
// Funciones de inicialización y gestión
void inicializarLista();
void CLI();
void print_error(const char *mensaje_error);

// Funciones de carga y creación de dataframes
void contarFilasYColumnas(const char *nombre_archivo, int *numFilas,
                          int *numColumnas);
void loadearCSV(const char *nombre_archivo);
int crearDF(Dataframe *df, int numColumnas, int numFilas,
            const char *nombre_df);
void liberarMemoriaDF(Dataframe *df);

// Funciones de procesamiento de datos
int contarColumnas(const char *line);
void cortarEspacios(char *str);
void leerEncabezados(FILE *file, Dataframe *df, int numColumnas);
void leerFilas(FILE *file, Dataframe *df, int numFilas, int numColumnas);

// Funciones de manipulación de dataframes
void agregarDF(Dataframe *nuevoDF);
void cambiarDF(Lista *lista, int indice);

// Funciones de interfaz de usuario
void metaCLI();
void viewCLI(int n);
void sortCLI(const char *nombre_col, int esta_desc);
void saveCLI(const char *nombre_archivo);
void filterCLI(Dataframe *df, const char *nombre_columna, const char *operador,
               void *valor);
void delnullCLI(const char *nombre_col);

// Funciones de procesamiento y validación
void verificarNulos(char *line, int fila, Dataframe *df);
int fechaValida(const char *str_fecha);
int compararValores(void *a, void *b, TipoDato tipo, int esta_desc);
void tiposColumnas(Dataframe *df);
void delcolumCLI(const char *nombre_col);

// Funciones de filtrado y manipulación de datos

void liberarListaCompleta(Lista *lista);
void procesarPorLotes(FILE *file, Dataframe *df, int tamanoLote);

int encontrarIndiceColumna(Dataframe *df, const char *nombre_columna);

void intercambiarFilas(Dataframe *df, int fila1, int fila2);

int comparar(void *dato1, void *dato2, TipoDato tipo, const char *operador);
void filterCLI(Dataframe *df, const char *nombre_columna, const char *operador,
               void *valor);

#endif