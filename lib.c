// contendrá las definiciones de todas las funciones cuyos prototipos se han declarado en “lib.h”.
// o sea lib.c es donde se implementan las funciones que main.c necesita ejecutar.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lib.h"

void leerCSV(const char *nombreArchivo) {
    FILE *file = fopen(nombreArchivo, "r");

    if (file == NULL) {
        perror("Error al abrir el archivo");
        return;
    }

    char *line = NULL;
    size_t len = 0;

    while (getline(&line, &len, file) != -1) {
        line[strcspn(line, "\n")] = 0; // Quitar el salto de línea

        char *token = strtok(line, ",");
        while (token != NULL) {
            printf("%s\t", token);
            token = strtok(NULL, ",");
        }
        printf("\n");
    }

    free(line);
    fclose(file);
}

void solicitarNombreArchivo(char *nombreArchivo, size_t size) {
    printf("Introduce el nombre del archivo CSV: ");
    scanf(" %1023s", nombreArchivo);
    strncat(nombreArchivo, ".csv", size - strlen(nombreArchivo) - 1);
}

void comandos(void) {
    char mode[30];
    char nombreArchivo[1024] = ""; 

    while (1) {
        printf("\nIntroduce el comando deseado: ");
        scanf(" %29s", mode);

        if (strcmp(mode, "load") == 0) {
            solicitarNombreArchivo(nombreArchivo, sizeof(nombreArchivo));
            leerCSV(nombreArchivo);
        } else if (strcmp(mode, "quit") == 0) {
            printf("\nEXIT PROGRAM\n");
            break;
        } else {
            printf("ERROR: Comando no reconocido. Intenta de nuevo.\n");
        }
    }
}
