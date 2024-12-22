#include <stdio.h>
#include <stdlib.h>

void print_csv(const char *filename) {
    FILE *file = fopen(filename, "r"); // Abrir el archivo en modo lectura
    if (!file) {
        perror("Error al abrir el archivo");
        exit(EXIT_FAILURE);
    }

    char buffer[1024]; // Buffer para leer cada línea del archivo
    while (fgets(buffer, sizeof(buffer), file)) {
        printf("%s", buffer); // Imprimir cada línea tal cual
    }

    fclose(file); // Cerrar el archivo
}

int main() {
    char filename[256];

    printf("Introduce el nombre del archivo CSV (con extensión): ");
    scanf("%s", filename);

    printf("Contenido del archivo CSV:\n");
    print_csv(filename);

    return 0;
}
