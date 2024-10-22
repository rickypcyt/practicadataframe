#include "lib.h"

#define MAX_COMMAND_LENGTH 100

void mostrarPrompt(Dataframe* dfActivo) {
    if (dfActivo == NULL) {
        printf("\033[1;37m[?]:> \033[0m");
    } else {
        printf("\033[1;37m[df%d: %d,%d]:> \033[0m", dfActivo->numColumnas, dfActivo->numFilas, dfActivo->numColumnas);
    }
}

int main() {
    Lista listaDataframes = {0, NULL};
    Dataframe* dfActivo = NULL;
    char comando[MAX_COMMAND_LENGTH];

    printf("\033[1;32mBienvenido al Manejador de Dataframes\n\033[0m");
    printf("\033[1;32mNombre: [Tu nombre]\n\033[0m");
    printf("\033[1;32mApellidos: [Tus apellidos]\n\033[0m");
    printf("\033[1;32mCorreo electrónico: [Tu correo]\n\n\033[0m");

    while (1) {
        mostrarPrompt(dfActivo);
        fgets(comando, MAX_COMMAND_LENGTH, stdin);
        comando[strcspn(comando, "\n")] = 0;  // Eliminar el salto de línea

        if (strcmp(comando, "quit") == 0) {
            printf("\033[1;32mEXIT PROGRAM\n\033[0m");
            eliminarListaDataframes(&listaDataframes);
            break;
        }

        // Aquí irían las implementaciones de los demás comandos
        // Por ejemplo:
        // else if (strncmp(comando, "load ", 5) == 0) {
        //     // Implementación del comando load
        // }
        // else if (strcmp(comando, "meta") == 0) {
        //     // Implementación del comando meta
        // }
        // ...

        else {
            printf("\033[1;31mComando no reconocido\n\033[0m");
        }
    }

    return 0;
}
