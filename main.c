#include <stdio.h>
#include <string.h>
#include <time.h>

// Colores para salida
#define BLK "\e[0;30m"
#define RED "\e[0;31m"
#define GRN "\e[0;32m"
#define YEL "\e[0;33m"
#define BLU "\e[0;34m"
#define MAG "\e[0;35m"
#define CYN "\e[0;36m"
#define WHT "\e[0;37m"

// Reset de color
#define reset "\e[0m"

// Tamaño máximo para la línea del archivo
#define MAX_LINE_LENGTH 1024

// Declaraciones de funciones
void comandosCLI();
void leerCSV(const char *nombreArchivo);
void solicitarNombreArchivo(char *nombreArchivo, size_t size);
void verificarNulos(char *linea); // verificar y corregir valores nulos

// Tipo enumerado para representar los diferentes tipos de datos en las columnas
typedef enum
{
    TEXTO,
    NUMERICO,
    FECHA
} TipoDato;

// Estructura para representar una columna del dataframe
typedef struct
{
    char nombre[30];       // Nombre de la columna
    TipoDato tipo;         // Tipo de datos de la columna (TEXTO, NUMERICO, FECHA)
    void *datos;           // Puntero genérico para almacenar los datos de la columna
    unsigned char *esNulo; // Array paralelo, indica valores nulos (1/0: nulo/noNulo)
    int numFilas;          // Número de filas en la columna
} Columna;

// Alias para tipos FECHA: 'Fecha' alias de 'struct tm'
typedef struct
{
    int year;
    int month;
    int day;
} Fecha;

// Estructura para representar un dataframe
typedef struct
{
    Columna *columnas;
    int numColumnas; // número de columnas
    int numFilas;
    int *indice; 
} Dataframe;

int main()
{
    comandosCLI(); // Llama a la función comandos
    return 0;
}

void comandosCLI()
{
    char mode[30];
    char nombreArchivo[MAX_LINE_LENGTH] = ""; // Inicializa el nombre del archivo
    printf("\nRicardo Perez mail\n");

    while (1)
    {
        printf(WHT "\nIntroduce el comando deseado: " reset);
        scanf(" %29s", mode);

        if (strcmp(mode, "load") == 0)
        {
            solicitarNombreArchivo(nombreArchivo, sizeof(nombreArchivo));
            leerCSV(nombreArchivo);
        }
        else if (strcmp(mode, "quit") == 0)
        {
            printf(GRN "\nFin...\n" reset WHT);
            break; // Salir del bucle
        }
        else if (strcmp(mode, "") == 0)
        {
            printf(GRN "\nEXIT PROGRAM\n" reset WHT);
            break; // Salir si se introduce una línea vacía
        }
        else
        {
            printf(RED "ERROR: Comando no reconocido. Intenta de nuevo.\n" reset WHT);
        }
    }
}

// Función para leer el archivo CSV
void leerCSV(const char *nombreArchivo)
{
    FILE *file = fopen(nombreArchivo, "r");

    if (file == NULL)
    {
        perror(RED "Error al abrir el archivo" reset WHT);
        return;
    }

    char linea[MAX_LINE_LENGTH];
    while (fgets(linea, sizeof(linea), file))
    {
        // Verificar y corregir valores nulos en la línea leída
        verificarNulos(linea);

        // Imprimir la línea corregida
        printf("%s", linea);
    }
    fclose(file);
}

// Función para solicitar el nombre del archivo CSV
void solicitarNombreArchivo(char *nombreArchivo, size_t size)
{
    printf("Introduce el nombre del archivo CSV: ");
    scanf(" %1023s", nombreArchivo);
    // Agregar la extensión .csv al nombre del archivo
    strncat(nombreArchivo, ".csv", size - strlen(nombreArchivo) - 1);
}

// verificar y corregir valores nulos en una línea
void verificarNulos(char *linea)
{
    char resultado[MAX_LINE_LENGTH * 2]; // almacenar la línea corregida
    int j = 0;
    int length = strlen(linea);

    // valor nulo al inicio
    if (linea[0] == ',')
    {
        resultado[j++] = '1'; // Reemplazar el valor nulo inicial con '1'
    }

    for (int i = 0; i < length; i++) // recorre linea original
    {
        // Detectar comas seguidas o coma al final de la línea
        if (linea[i] == ',' && (linea[i + 1] == ',' || linea[i + 1] == '\n'))
        {
            resultado[j++] = ','; // Mantener la coma
            resultado[j++] = '1'; // Reemplazar el valor nulo con '1'
        }
        else
        {
            resultado[j++] = linea[i]; // j es el nuevo output
        }
    }

    resultado[j] = '\0'; // Terminar la cadena resultante

    // Sobrescribir la línea original con la corregida
    strcpy(linea, resultado);
}
