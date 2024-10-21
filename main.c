#include "lib.h"

int main() {
    comandos(); // Llama a la función comandos
    return 0;
}

void comandos() {
    char mode[30];
    char nombreArchivo[MAX_LINE_LENGTH] = ""; // Inicializa el nombre del archivo
    printf("\nRicardo Perez rickypcyt@gmail.com\n");

    while (1) {
        printf(WHT "\nIntroduce el comando deseado: " reset);
        scanf(" %29s", mode);

        if (strcmp(mode, "load") == 0) {
            solicitarNombreArchivo(nombreArchivo, sizeof(nombreArchivo));
            leerCSV(nombreArchivo);
        } else if (strcmp(mode, "quit") == 0) {
            printf(GRN "\nEXIT PROGRAM\n" reset WHT);
            break; // Salir del bucle
        } else if (strcmp(mode, "") == 0) {
            printf(GRN "\nEXIT PROGRAM\n" reset WHT);
            break; // Salir si se introduce una línea vacía
        } else {
            printf(RED "ERROR: Comando no reconocido. Intenta de nuevo.\n" reset WHT);
        }
    }
}

void leerCSV(const char *nombreArchivo) {
    FILE *file = fopen(nombreArchivo, "r");
    // r = read

    if (file == NULL) {
        perror(RED "Error al abrir el archivo" reset WHT);
        return;
    }

    char *line = NULL;
    size_t len = 0;

    // Esta línea inicia un bucle que lee cada línea del archivo hasta que llegue al final
    while (getline(&line, &len, file) != -1) {
        // Quita el salto de línea y trunca la cadena
        line[strcspn(line, "\n")] = 0;

        // Tokenizar la línea
        char *token = strtok(line, ",");
        while (token != NULL) {
            printf("%s\t", token);     // imprime token
            token = strtok(NULL, ","); // pasa al siguiente
        }

        printf("\n");
    }

    free(line); // Liberar la memoria asignada por getline
    fclose(file);
}

void solicitarNombreArchivo(char *nombreArchivo, size_t size) {
    printf("Introduce el nombre del archivo CSV: ");
    scanf(" %1023s", nombreArchivo);
    // Agregar la extensión .csv al nombre del archivo
    strncat(nombreArchivo, ".csv", size - strlen(nombreArchivo) - 1);
}

void crearDataframe() {
    // Implementación de la función crearDataframe (si es necesario)
}
