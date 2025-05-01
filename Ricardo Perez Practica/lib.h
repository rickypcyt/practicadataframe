// lib.h
#ifndef LIB_H
#define LIB_H

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

// Constantes de configuración del sistema
#define MAX_LINE_LENGTH 4096
#define MAX_COLUMNS 1000
#define MAX_ROWS 5000
#define MAX_FILENAME 256
#define MAX_NOMBRE_COLUMNA 50
#define MAX_INDICE_LENGTH 20
#define BATCH_SIZE 5000

// Códigos de color ANSI para salida por consola
#define RED "\x1b[31m"
#define GREEN "\x1b[32m"
#define WHITE "\x1b[37m"
#define RESET "\x1b[0m"

// Macro para validación de parámetros
#define VALIDAR_DF_Y_PARAMETROS(df, param) \
    do { \
        if (!df || !param) { \
            print_error("No hay df activo o parámetros inválidos"); \
            return; \
        } \
    } while(0)

// Enumeración de tipos de datos soportados
typedef enum {
    TEXTO,     // Tipo cadena de texto
    NUMERICO,  // Tipo numérico (punto flotante)
    FECHA      // Tipo fecha
} TipoDato;

// Estado de nulidad de datos
typedef enum {
    NO_NULO = 0,
    NULO = 1
} EstadoNulo;

// Estructura de Columna: Representa una columna en el df
typedef struct {
    char nombre[MAX_NOMBRE_COLUMNA];  // Nombre de la columna
    TipoDato tipo;                    // Tipo de datos de la columna
    char **datos;                     // Arreglo de punteros a datos
    EstadoNulo *esNulo;              // Indicador de valores nulos
} Columna;

// Estructura de df: Contenedor principal de datos
typedef struct {
    Columna *columnas;           // Arreglo de columnas
    int numColumnas;            // Número total de columnas
    int numFilas;              // Número total de filas
    char indice[MAX_INDICE_LENGTH];  // Nombre del df
} Dataframe;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista {
    Dataframe *df;                // puntero a Dataframe
    struct NodoLista *siguiente;  // Puntero al siguiente nodo de la lista
} Nodo;

// Estructura para representar la lista de Dataframe's
typedef struct {
    int numDFs;     // Número de Dataframes almacenados en la lista
    Nodo *primero;  // Puntero al primer Nodo de la lista
} Lista;

// Variables globales para gestión del sistema
extern Lista listaDF;         // Declaración de la variable global
extern Dataframe *df_actual;  // Declaración de la variable global
extern char
    promptTerminal[MAX_LINE_LENGTH];  // Declaración de la variable global

// Funciones helper para manejo de memoria y datos
Dataframe* crearNuevoDataframe(int numColumnas, int numFilas, const char* indice);
void copiarNombreColumna(char* destino, const char* origen);
char* copiarDatoSeguro(const char* origen);
void actualizarPrompt(const Dataframe* df);
void liberarRecursosEnError(Dataframe* df, const char* mensaje);

// Funciones de inicialización y gestión
void inicializarLista(void);
void CLI(void);
void print_error(const char *mensaje_error);

// Funciones de carga y creación de dataframes
void contarFilasYColumnas(const char *nombre_archivo, int *numFilas, int *numColumnas);
void loadearCSV(const char *nombre_archivo);
int crearDF(Dataframe *df, int numColumnas, int numFilas, const char *nombre_df);
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
void metaCLI(void);
void viewCLI(int n);
void sortCLI(const char *nombre_col, int esta_desc);
void saveCLI(const char *nombre_archivo);
void filterCLI(Dataframe *df, const char *nombre_columna, const char *operador, void *valor);
void delnullCLI(const char *nombre_col);
void quarterCLI(const char *nombre_columna_fecha, const char *nombre_nueva_columna);

// Funciones de procesamiento y validación
int fechaValida(const char *str_fecha);
void verificarNulos(char *lineaLeida, int fila, Dataframe *df, char *resultado);
int compararValores(void *a, void *b, TipoDato tipo, int esta_desc);
void tiposColumnas(Dataframe *df);
void delcolumCLI(const char *nombre_col);

// Funciones de filtrado y manipulación de datos
void liberarListaCompleta(Lista *lista);
void procesarPorLotes(FILE *file, Dataframe *df, int tamanoLote);
int encontrarIndiceColumna(Dataframe *df, const char *nombre_columna);
void intercambiarFilas(Dataframe *df, int fila1, int fila2);
int comparar(void *dato1, void *dato2, TipoDato tipo, const char *operador);

#endif
