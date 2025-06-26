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
#define MAX_NOMBRE_COLUMNA 30
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

// Tipo enumerado para representar los diferentes tipos de datos en las columnas
typedef enum {
    TEXTO,
    NUMERICO,
    FECHA
} TipoDato;

// Estado de nulidad de datos
typedef enum {
    NO_NULO = 0,
    NULO = 1
} EstadoNulo;

// Estructura para representar una columna del dataframe
typedef struct {
    char nombre[30];            // Nombre de la columna
    TipoDato tipo;              // Tipo de datos de la columna (TEXTO, NUMERICO, FECHA)
    char **datos;               // Array de punteros a datos
    EstadoNulo *esNulo;         // Array paralelo, indica valores nulos
    int numFilas;               // Número de filas en la columna
} Columna;

// Estructura para representar el dataframe como un conjunto de columnas
typedef struct {
    Columna *columnas;          // Array de columnas (con tipos de datos distintos)
    int numColumnas;            // Número de columnas en el dataframe
    int numFilas;               // Número de filas (igual para todas las columnas)
    char indice[MAX_INDICE_LENGTH];  // Nombre del dataframe
    char nombre[51];            // Nombre único del dataframe (nuevo campo)
} Dataframe;

// Alias para tipo FECHA: 'Fecha' alias de 'struct tm' (#include <time.h>)
typedef struct tm Fecha;

// Estructura para representar un nodo de la lista
typedef struct NodoLista {
    Dataframe *df;              // Puntero a un dataframe
    struct NodoLista *siguiente; // Puntero al siguiente nodo de la lista
} Nodo;

// Estructura para representar la lista de dataframes
typedef struct {
    int numDFs;                 // Número de dataframes almacenados en la lista
    Nodo *primero;             // Puntero al primer Nodo de la lista
} Lista;

// Variables globales para gestión del sistema
extern Lista listaDF;         // Declaración de la variable global
extern Dataframe *dfActual;  // Declaración de la variable global
extern char promptTerminal[MAX_LINE_LENGTH];  // Declaración de la variable global

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
void loadearCSV(const char *nombre_archivo, char sep);
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
void cambiarDFPorNombre(Lista *lista, const char *nombre);
int nombreDFUnico(const Lista *lista, const char *nombre);

// Funciones de interfaz de usuario
void metaCLI(void);
void viewCLI(int n);
void viewNegativeCLI(int n);
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

// Funciones de copia seguras y reutilizables
/**
 * Copia una columna completa de origen a destino, incluyendo datos y estados nulos.
 * @param destino Columna destino
 * @param origen Columna origen (const)
 * @param numFilas Número de filas a copiar
 * @return 1 si la copia fue exitosa, 0 en caso de error
 */
int copiarColumna(Columna *destino, const Columna *origen, int numFilas);

/**
 * Copia una fila completa de un dataframe origen a uno destino.
 * @param destino Dataframe destino
 * @param origen Dataframe origen (const)
 * @param fila_origen Índice de la fila en el dataframe origen
 * @param fila_destino Índice de la fila en el dataframe destino
 * @return 1 si la copia fue exitosa, 0 en caso de error
 */
int copiarFila(Dataframe *destino, const Dataframe *origen, int fila_origen, int fila_destino);

void prefixCLI(const char *nombre_col, int n, const char *nombre_nueva_col);
void listCLI(void);

void addCLI(const char *nombre_archivo, char sep);

#endif
