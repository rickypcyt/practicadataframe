#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

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
void verificarNulos(char *linea);
void contarFilasYColumnas(FILE *file, int *numFilas, int *numColumnas);

// Definición de las estructuras (como antes)
typedef enum
{
    TEXTO,
    NUMERICO,
    FECHA
} TipoDato;

typedef struct
{
    char nombre[30];
    TipoDato tipo;
    void *datos;
    unsigned char *esNulo;
    int numFilas;
} Columna;

typedef struct
{
    Columna *columnas;
    int numColumnas;
    int numFilas;
} Dataframe;

// Variables globales
Dataframe *dfActivo = NULL; // Puntero al dataframe activo
char prompt[MAX_LINE_LENGTH] = "[?]:> "; // Prompt inicial

int main()
{
    comandosCLI(); // Llama a la función comandosCLI
    return 0;
}

void comandosCLI()
{
    char mode[30];
    char nombreArchivo[MAX_LINE_LENGTH] = "";

    printf("\nRicardo Perez mail\n");

    while (1)
    {
        // Mostrar el prompt actualizado
        printf(WHT "%s" reset, prompt);

        scanf(" %29s", mode);

        if (strcmp(mode, "load") == 0)
        {
            solicitarNombreArchivo(nombreArchivo, sizeof(nombreArchivo));
            leerCSV(nombreArchivo);
        }
        else if (strcmp(mode, "quit") == 0)
        {
            printf(GRN "\nFin...\n" reset WHT);
            break;
        }
        else if (strcmp(mode, "") == 0)
        {
            printf(GRN "\nEXIT PROGRAM\n" reset WHT);
            break;
        }
        else
        {
            printf(RED "ERROR: Comando no reconocido. Intenta de nuevo.\n" reset WHT);
        }
    }
}

// Función para leer el archivo CSV y crear un dataframe temporal
void leerCSV(const char *nombreArchivo)
{
    FILE *file = fopen(nombreArchivo, "r");

    if (file == NULL)
    {
        perror(RED "Error al abrir el archivo" reset WHT);
        return;
    }

    char linea[MAX_LINE_LENGTH];
    int numColumnas = 0;
    int numFilas = 0;

    contarFilasYColumnas(file, &numFilas, &numColumnas);

    // Crear el dataframe temporal df0
    Dataframe *df0 = (Dataframe *)malloc(sizeof(Dataframe));
    df0->numFilas = numFilas;
    df0->numColumnas = numColumnas;
    df0->columnas = (Columna *)malloc(numColumnas * sizeof(Columna));

    // Leer la primera línea para obtener los nombres de las columnas
    rewind(file);
    fgets(linea, sizeof(linea), file);
    verificarNulos(linea);

    int colIndex = 0;
    for (char *token = strtok(linea, ","); token != NULL; token = strtok(NULL, ","))
    {
        snprintf(df0->columnas[colIndex].nombre, sizeof(df0->columnas[colIndex].nombre), "%s", token);
        df0->columnas[colIndex].tipo = TEXTO;
        df0->columnas[colIndex].numFilas = numFilas;
        df0->columnas[colIndex].datos = malloc(numFilas * sizeof(char *));
        df0->columnas[colIndex].esNulo = malloc(numFilas * sizeof(unsigned char));
        colIndex++;
    }

    // Leer el resto del archivo y llenar los datos en las columnas
    int filaIndex = 0;
    while (fgets(linea, sizeof(linea), file))
    {
        verificarNulos(linea);
        colIndex = 0;
        for (char *token = strtok(linea, ","); token != NULL; token = strtok(NULL, ","))
        {
            ((char **)df0->columnas[colIndex].datos)[filaIndex] = strdup(token);
            df0->columnas[colIndex].esNulo[filaIndex] = (token[0] == '1') ? 1 : 0;
            colIndex++;
        }
        filaIndex++;
    }

    fclose(file);

    // Asignar df0 como el dataframe activo
    dfActivo = df0;

    // Actualizar el prompt con el nombre del dataframe y sus dimensiones
    snprintf(prompt, sizeof(prompt), "[df0: %d,%d]:> ", dfActivo->numFilas, dfActivo->numColumnas);

    printf(GRN "\nDataframe df0 creado con éxito. El archivo tiene %d filas y %d columnas.\n" reset, df0->numFilas, df0->numColumnas);
}

// Función para solicitar el nombre del archivo CSV
void solicitarNombreArchivo(char *nombreArchivo, size_t size)
{
    printf("Introduce el nombre del archivo CSV: ");
    scanf(" %1023s", nombreArchivo);
    strncat(nombreArchivo, ".csv", size - strlen(nombreArchivo) - 1);
}

// Función para verificar y corregir valores nulos en la línea
void verificarNulos(char *linea)
{
    char resultado[MAX_LINE_LENGTH * 2];
    int j = 0;
    int length = strlen(linea);

    if (linea[0] == ',')
    {
        resultado[j++] = '1';
    }

    for (int i = 0; i < length; i++)
    {
        if (linea[i] == ',' && (linea[i + 1] == ',' || linea[i + 1] == '\0'))
        {
            resultado[j++] = ',';
            resultado[j++] = '1';
        }
        else
        {
            resultado[j++] = linea[i];
        }
    }

    resultado[j] = '\0';
    strcpy(linea, resultado);
}

// Función para contar las filas y columnas del archivo CSV
void contarFilasYColumnas(FILE *file, int *numFilas, int *numColumnas)
{
    char linea[MAX_LINE_LENGTH];
    *numFilas = 0;
    *numColumnas = 0;

    rewind(file);

    if (fgets(linea, sizeof(linea), file))
    {
        verificarNulos(linea);

        for (char *token = strtok(linea, ","); token != NULL; token = strtok(NULL, ","))
        {
            (*numColumnas)++;
        }

        (*numFilas)++;
    }

    while (fgets(linea, sizeof(linea), file))
    {
        verificarNulos(linea);
        (*numFilas)++;
    }
}
