#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>  // Added time module for date parsing

// Constants and Configuration
#define MAX_LINE_LENGTH 1024
#define MAX_COLUMNS 100
#define MAX_FILENAME 256

// Color Codes for Better Console Output
#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_WHITE   "\x1b[37m"
#define ANSI_COLOR_RESET   "\x1b[0m"

// Enum for Data Types
typedef enum {
    TYPE_TEXT,
    TYPE_NUMERIC,
    TYPE_DATE
} DataType;

// Improved Column Structure
typedef struct {
    char name[50];
    DataType type;
    void **data;
    int *is_null;
    int row_count;
} Column;

// Improved Dataframe Structure
typedef struct {
    Column *columns;
    int column_count;
    int row_count;
    char name[20];
} DataFrame;

// Global Variables
DataFrame *active_dataframe = NULL;
char current_prompt[MAX_LINE_LENGTH] = "[?]:> ";

// Function Prototypes
void initialize_dataframe(DataFrame *df, int columns, int rows, const char *name);
void free_dataframe(DataFrame *df);
int count_csv_columns(const char *line);
void trim_whitespace(char *str);
void load_csv(const char *filename);
void show_dataframe();
void print_error(const char *message);
void interactive_cli();
void detect_column_types(DataFrame *df);

// Main Function
int main() {
    printf(ANSI_COLOR_GREEN "Dataframe Management System\n" ANSI_COLOR_RESET);
    interactive_cli();
    return 0;
}

// Enhanced Error Printing Function
void print_error(const char *message) {
    fprintf(stderr, ANSI_COLOR_RED "ERROR: %s\n" ANSI_COLOR_RESET, message);
}

// Initialize Dataframe with Memory Allocation
void initialize_dataframe(DataFrame *df, int columns, int rows, const char *name) {
    df->columns = malloc(columns * sizeof(Column));
    if (!df->columns) {
        print_error("Memory allocation failed for columns");
        return;
    }
    df->column_count = columns;
    df->row_count = rows;
    strncpy(df->name, name, sizeof(df->name) - 1);

    for (int i = 0; i < columns; i++) {
        df->columns[i].data = malloc(rows * sizeof(void*));
        df->columns[i].is_null = malloc(rows * sizeof(int));
        df->columns[i].row_count = rows;
        memset(df->columns[i].is_null, 0, rows * sizeof(int));
    }
}

// Free Dataframe Memory
void free_dataframe(DataFrame *df) {
    if (df) {
        for (int i = 0; i < df->column_count; i++) {
            for (int j = 0; j < df->row_count; j++) {
                free(df->columns[i].data[j]);
            }
            free(df->columns[i].data);
            free(df->columns[i].is_null);
        }
        free(df->columns);
        free(df);
    }
}

// Trim Whitespace from String
void trim_whitespace(char *str) {
    char *start = str;
    char *end = str + strlen(str) - 1;

    while (isspace(*start)) start++;
    while (end > start && isspace(*end)) end--;

    *(end + 1) = '\0';
    memmove(str, start, end - start + 2);
}

// Count Columns in CSV Line
int count_csv_columns(const char *line) {
    int count = 1;
    for (int i = 0; line[i]; i++) {
        if (line[i] == ',') count++;
    }
    return count;
}

// Handle null values in CSV
void handle_null_values(char *linea) {
    char resultado[MAX_LINE_LENGTH * 2] = {0};
    int j = 0;
    int length = strlen(linea);
    int expecting_value = 1;  // Start expecting a value

    for (int i = 0; i <= length; i++) {
        // If we encounter a comma, carriage return, or newline, process the field
        if (i == length || linea[i] == ',' || linea[i] == '\r') {
            if (expecting_value) {
                // Insert 'NULL' for empty fields
                resultado[j++] = 'N';
                resultado[j++] = 'U';
                resultado[j++] = 'L';
                resultado[j++] = 'L';
            }
            
            // If it's not the last character, copy the comma, carriage return, or newline
            if (i < length) {
                resultado[j++] = linea[i];  // Add comma, carriage return, or newline
            }

            // After a comma, carriage return, or newline, we expect a value for the next field
            expecting_value = 1;

            // If we encounter \n or \r, treat them as the end of a line
            if (linea[i] == '\n' || linea[i] == '\r') {
                // We might want to handle line breaks properly for Windows-based CSVs
                resultado[j++] = '\r';  // Carriage return (if needed)
                resultado[j++] = '\n';  // Newline
            }
        } else {
            resultado[j++] = linea[i];
            expecting_value = 0;  // We found a non-empty value, so no longer expecting 'NULL'
        }
    }

    resultado[j] = '\0';  // Terminate the resulting string
    strcpy(linea, resultado); // Overwrite original line
}




// Load CSV and parse data into DataFrame
void load_csv(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        print_error("Cannot open file");
        return;
    }

    char line[MAX_LINE_LENGTH];
    int row_count = 0, column_count = 0;

    // First pass: Count rows and columns
    while (fgets(line, sizeof(line), file)) {
        handle_null_values(line);  // Process null values
        row_count++;
        if (row_count == 1) {
            column_count = count_csv_columns(line);
        }
    }
    rewind(file);

    // Free existing dataframe if present
    if (active_dataframe) {
        free_dataframe(active_dataframe);
    }

    // Allocate new dataframe
    active_dataframe = malloc(sizeof(DataFrame));
    if (!active_dataframe) {
        print_error("Memory allocation failed for DataFrame");
        fclose(file);
        return;
    }
    initialize_dataframe(active_dataframe, column_count, row_count, "df0");

    // Read headers
    fgets(line, sizeof(line), file);
    char *token = strtok(line, ",");
    for (int col = 0; token != NULL; col++) {
        trim_whitespace(token);
        strncpy(active_dataframe->columns[col].name, token, sizeof(active_dataframe->columns[col].name) - 1);
        token = strtok(NULL, ",");
    }

    // Read data rows
    int current_row = 0;
while (fgets(line, sizeof(line), file) && current_row < row_count - 1) {
    handle_null_values(line);  // Process null values
    token = strtok(line, ",");
    for (int col = 0; token != NULL; col++) {
        trim_whitespace(token);

        // Special handling for null values
        if (strcmp(token, "NULL") == 0) {
            active_dataframe->columns[col].data[current_row] = NULL;
            active_dataframe->columns[col].is_null[current_row] = 1;
        } else {
            active_dataframe->columns[col].data[current_row] = strdup(token);
            active_dataframe->columns[col].is_null[current_row] = 0;
        }
        token = strtok(NULL, ",");
    }
    current_row++;
}



    // Detect column types
    detect_column_types(active_dataframe);

    // Update prompt
    snprintf(current_prompt, sizeof(current_prompt), "[%s: %d,%d]:> ", active_dataframe->name, active_dataframe->row_count, active_dataframe->column_count);

    printf(ANSI_COLOR_GREEN "Successfully loaded %s with %d rows and %d columns\n" ANSI_COLOR_RESET, filename, active_dataframe->row_count, active_dataframe->column_count);

    fclose(file);
}

// Validate date format YYYY-MM-DD
int is_valid_date(const char *date_str) {
    struct tm tm;
    memset(&tm, 0, sizeof(struct tm));

    // Use sscanf as an alternative to strptime for Windows compatibility
    if (sscanf(date_str, "%4d-%2d-%2d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday) != 3) {
        return 0;
    }

    tm.tm_year -= 1900;  // Year since 1900
    tm.tm_mon -= 1;      // Month (0-11)
    time_t t = mktime(&tm);
    return t != -1;
}


// Detect column types: Date > Numeric > Text
void detect_column_types(DataFrame *df) {
    for (int col = 0; col < df->column_count; col++) {
        df->columns[col].type = TYPE_TEXT;  // Default to text
        int is_numeric = 1;
        int is_date = 1;

        for (int row = 0; row < df->row_count; row++) {
            char *value = (char*)df->columns[col].data[row];
            if (value == NULL) continue;

            // Check if numeric
            char *endptr;
            strtod(value, &endptr);
            if (endptr == value) {
                is_numeric = 0;
            }

            // Check if date
            if (!is_valid_date(value)) {
                is_date = 0;
            }
        }

        // Determine column type priority: Date > Numeric > Text
        if (is_date) {
            df->columns[col].type = TYPE_DATE;
        } else if (is_numeric) {
            df->columns[col].type = TYPE_NUMERIC;
        }
    }
}

// Show DataFrame in a tabular format
void show_dataframe() {
    if (!active_dataframe) {
        print_error("No dataframe loaded.");
        return;
    }

    printf(ANSI_COLOR_WHITE "Dataframe: %s\n" ANSI_COLOR_RESET, active_dataframe->name);
    for (int col = 0; col < active_dataframe->column_count; col++) {
        printf("%-20s", active_dataframe->columns[col].name);
    }
    printf("\n");

    for (int row = 0; row < active_dataframe->row_count; row++) {
        for (int col = 0; col < active_dataframe->column_count; col++) {
            char *value = (char*)active_dataframe->columns[col].data[row];
            if (value == NULL) {
                printf("NULL                 ");  // Adjust spacing if needed
            } else {
                printf("%-20s", value);
            }
        }
        printf("\n");
    }
}



// Interactive CLI for User Interaction
void interactive_cli() {
    char input[MAX_LINE_LENGTH];
    while (1) {
        printf("%s", current_prompt);
        fgets(input, sizeof(input), stdin);
        trim_whitespace(input);

        if (strcmp(input, "quit") == 0) {
            break;
        } else if (strncmp(input, "load ", 5) == 0) {
            load_csv(input + 5);
        } else if (strcmp(input, "show") == 0) {
            show_dataframe();
        } else {
            printf("Unknown command: %s\n", input);
        }
    }
}
