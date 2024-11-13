#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 1024
#define RED "\x1B[31m"
#define RESET "\x1B[0m"
#define WHT "\x1B[37m"

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

// Función para contar filas y columnas de un archivo CSV
void contarFilasYColumnas(FILE *file, int *numFilas, int *numColumnas)
{
    char linea[MAX_LINE_LENGTH];

    // Leer la primera línea para contar las columnas
    if (fgets(linea, sizeof(linea), file)) {
        verificarNulos(linea);  // Verifica nulos en la primera línea

        // Contar columnas en la primera línea
        for (char *token = strtok(linea, ","); token != NULL; token = strtok(NULL, ",")) {
            (*numColumnas)++;
        }

        (*numFilas)++;  // Contamos la primera línea como una fila
        printf("%s", linea); // Imprimir la primera línea
    }

    // Contar el número de filas e imprimir cada línea
    while (fgets(linea, sizeof(linea), file)) {
        verificarNulos(linea);  // Verificar nulos para cada línea
        printf("%s", linea);     // Imprimir la línea
        (*numFilas)++;           // Contar la línea leída
    }
}

// Función para leer el archivo CSV
void leerCSV(const char *nombreArchivo)
{
    FILE *file = fopen(nombreArchivo, "r");
    if (file == NULL)
    {
        perror(RED "Error al abrir el archivo" RESET WHT);
        return;
    }

    int numColumnas = 0;
    int numFilas = 0;

    contarFilasYColumnas(file, &numFilas, &numColumnas); // Llamar a la función de conteo

    // Mostrar el número de filas y columnas
    printf("\nEl archivo tiene %d filas y %d columnas.\n", numFilas, numColumnas);

    // Cerrar el archivo
    fclose(file);
}

int main() {
    // Llamada de ejemplo a la función leerCSV
    leerCSV("datos.csv");
    return 0;
}
