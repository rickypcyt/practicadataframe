// contendrá las definiciones de todas las funciones cuyos prototipos se han declarado en “lib.h”.
// o sea lib.c es donde se implementan las funciones que main.c necesita ejecutar.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "lib.h"

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

void comandos()
{
    char mode[30];
    char nombreArchivo[MAX_LINE_LENGTH] = ""; // Inicializa el nombre del archivo
    printf("\nRicardo Perez rickypcyt@gmail.com\n");

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
            printf(GRN "\nEXIT PROGRAM\n" reset WHT);
            break; // Salir del bucle
        }
        else if (strcmp(mode, "") == 0)
        {
            printf(GRN "\nEXIT PROGRAM\n" reset WHT);
        }
        else
        {
            printf(RED "ERROR: Comando no reconocido. Intenta de nuevo.\n" reset WHT);
        }
    }
}

void leerCSV(const char *nombreArchivo)
{
    FILE *file = fopen(nombreArchivo, "r");
    // r = read

    if (file == NULL)
    {
        perror(RED "Error al abrir el archivo" reset WHT);
        return;
    }

    char *line = NULL;
    size_t len = 0;

    // Esta línea inicia un bucle que lee cada línea del archivo hasta que llegue al final
    while (getline(&line, &len, file) != -1)
    {
        // Quita el salto de línea y trunca la cadena
        line[strcspn(line, "\n")] = 0;

        // Tokenizar la línea
        char *token = strtok(line, ",");
        while (token != NULL)
        {
            printf("%s\t", token);     // imprime token
            token = strtok(NULL, ","); // pasa al siguiente
        }

        printf("\n");
    }

    free(line); // Liberar la memoria asignada por getline
    fclose(file);
}

void solicitarNombreArchivo(char *nombreArchivo, size_t size)
{
    printf("Introduce el nombre del archivo CSV: ");
    scanf(" %1023s", nombreArchivo);
    // Agregar la extensión .csv al nombre del archivo
    strncat(nombreArchivo, ".csv", size - strlen(nombreArchivo) - 1);
}

void crearDataframe()
{
}