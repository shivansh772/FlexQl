#include <stdio.h>

#include "flexql.h"

static int callback(void *data, int columnCount, char **values, char **columnNames) {
    (void)data;
    for (int i = 0; i < columnCount; ++i) {
        printf("%s = %s\n", columnNames[i], values[i] ? values[i] : "NULL");
    }
    printf("\n");
    return 0;
}

int main(void) {
    FlexQL *db = 0;
    char *errMsg = 0;

    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
        printf("Cannot connect to FlexQL server\n");
        return 1;
    }

    printf("Connected to FlexQL server\n");

    if (flexql_exec(db, "CREATE TABLE STUDENT(ID DECIMAL, NAME VARCHAR);", 0, 0, &errMsg) != FLEXQL_OK) {
        printf("SQL error: %s\n", errMsg);
        flexql_free(errMsg);
    }

    if (flexql_exec(db, "INSERT INTO STUDENT VALUES (1, 'Alice');", 0, 0, &errMsg) != FLEXQL_OK) {
        printf("SQL error: %s\n", errMsg);
        flexql_free(errMsg);
    }

    if (flexql_exec(db, "INSERT INTO STUDENT VALUES (2, 'Bob');", 0, 0, &errMsg) != FLEXQL_OK) {
        printf("SQL error: %s\n", errMsg);
        flexql_free(errMsg);
    }

    if (flexql_exec(db, "SELECT * FROM STUDENT;", callback, 0, &errMsg) != FLEXQL_OK) {
        printf("SQL error: %s\n", errMsg);
        flexql_free(errMsg);
    }

    flexql_close(db);
    return 0;
}
