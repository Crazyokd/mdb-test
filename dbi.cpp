#include "dbi.h"
#include "util.h"

static inline mongoc_cursor_t *cross_platform_mongoc_collection_find(
    mongoc_collection_t *collection, const bson_t *query)
{
#if MONGOC_MAJOR_VERSION > 1 \
    || (MONGOC_MAJOR_VERSION == 1 && MONGOC_MINOR_VERSION >= 5)
    return mongoc_collection_find_with_opts(collection, query, NULL, NULL);
#else
    return mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0, query,
                                  NULL, NULL);
#endif
}

static inline bool cross_platform_mongoc_collection_insert(
    mongoc_collection_t *collection, const bson_t *doc, bson_error_t *error)
{
#if MONGOC_MAJOR_VERSION > 1 \
    || (MONGOC_MAJOR_VERSION == 1 && MONGOC_MINOR_VERSION >= 5)
    return mongoc_collection_insert_one(collection, doc, NULL, NULL, error);
#else
    return mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL,
                                    error);
#endif
}

static inline bool cross_platform_mongoc_collection_insert_many(
    mongoc_collection_t *collection, const bson_t **docs, size_t size,
    bson_error_t *error)
{
#if MONGOC_MAJOR_VERSION > 1 \
    || (MONGOC_MAJOR_VERSION == 1 && MONGOC_MINOR_VERSION >= 5)
    return mongoc_collection_insert_many(collection, docs, size, NULL, NULL,
                                         error);
#else
    for (int i = 0; i < size; i++) {
        return mongoc_collection_insert(collection, MONGOC_INSERT_NONE, docs[i],
                                        NULL, error);
    }
#endif
}

static inline bool cross_platform_mongoc_collection_update(
    mongoc_collection_t *collection, const bson_t *query, const bson_t *update,
    bson_error_t *error)
{
#if MONGOC_MAJOR_VERSION > 1 \
    || (MONGOC_MAJOR_VERSION == 1 && MONGOC_MINOR_VERSION >= 5)
    return mongoc_collection_update_one(collection, query, update, NULL, NULL,
                                        error);
#else
    return mongoc_collection_update(collection, MONGOC_UPDATE_NONE, query,
                                    update, NULL, error);
#endif
}

int insert_data_directly(const char *json_data, mongoc_collection_t *c)
{
    bson_error_t error;
    bson_t *bson = bson_new_from_json((const uint8_t *)json_data, -1, &error);
    if (!bson) {
        fprintf(stderr, "%s\n", error.message);
        return EXIT_FAILURE;
    }
    if (!cross_platform_mongoc_collection_insert(c, bson, &error)) {
        fprintf(stderr, "%s\n", error.message);
        return EXIT_FAILURE;
    }
    bson_destroy(bson);
    return EXIT_SUCCESS;
}

static int insert_many_data_directly(const char **datas, size_t size, mongoc_collection_t *c)
{
    bson_error_t error;
    /* todo: allocate docs externally */
    bson_t **docs = (bson_t **)malloc(sizeof(bson_t *) * size);
    if (!docs) {
        return EXIT_FAILURE;
    }
    size_t real_size = size;
    int idx = 0;

    for (int i = 0; i < size; i++) {
        docs[idx] = bson_new_from_json((const uint8_t *)datas[i], -1, &error);
        if (!docs[idx]) {
            /* we do not exit here */
            fprintf(stderr, "%s\n", error.message);
            real_size--;
        } else {
            idx++;
        }
    }
    cross_platform_mongoc_collection_insert_many(c, docs, real_size, &error);

    for (int i = 0; i < real_size; i++) {
        bson_destroy(docs[i]);
    }
    free(docs);
    return EXIT_SUCCESS;
}

int insert_many_docs_directly(bson_t **docs, size_t size, mongoc_collection_t *c)
{
    cross_platform_mongoc_collection_insert_many(c, docs, size, NULL);

    for (int i = 0; i < size; i++) {
        bson_destroy(docs[i]);
    }
    return EXIT_SUCCESS;
}

static int update_db_by_query(bson_t *query, bson_t *update,
                              mongoc_collection_t *c, bson_error_t *error)
{
    if (!update) {
        fprintf(stderr, "update object is null.\n");
        return EXIT_FAILURE;
    }
    if (!cross_platform_mongoc_collection_update(c, query, update, error)) {
        fprintf(stderr, "%s\n", error->message);
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

int update_or_insert_data_by_query(bson_t *query, const char *json_data,
                                   mongoc_collection_t *c)
{
    mongoc_cursor_t *cursor = cross_platform_mongoc_collection_find(c, query);
    bson_error_t error;

    bson_t *doc;
    bson_t *bson = bson_new_from_json((const uint8_t *)json_data, -1, &error);
    if (!bson) {
        fprintf(stderr, "%s\n", error.message);
        return EXIT_FAILURE;
    }
    if (!mongoc_cursor_next(cursor, &doc)) {
        lol_debug("insert new record to db\n");
        if (!cross_platform_mongoc_collection_insert(c, bson, &error)) {
            fprintf(stderr, "%s\n", error.message);
            return EXIT_FAILURE;
        }
    } else {
        lol_debug("update record for db\n");
        bson_t *update = BCON_NEW("$set", BCON_DOCUMENT(bson));
        update_db_by_query(query, update, c, &error);

        bson_destroy(update);
    }

    bson_destroy(bson);
    bson_destroy(doc);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
}

int update_data_by_query(bson_t *query, const char *json_data,
                         mongoc_collection_t *c)
{
    bson_error_t error;

    bson_t *bson = bson_new_from_json((const uint8_t *)json_data, -1, &error);
    if (!bson) {
        bson_destroy(query);
        fprintf(stderr, "%s\n", error.message);
        return EXIT_FAILURE;
    }
    bson_t *update = BCON_NEW("$set", BCON_DOCUMENT(bson));
    bson_t *opt = BCON_NEW("upsert", BCON_BOOL(true));
    mongoc_collection_update_one(c, query, update, opt, NULL, &error);
    bson_destroy(opt);
    bson_destroy(update);

    bson_destroy(bson);
    bson_destroy(query);
}

int update_or_insert_line_info(bson_t *query, const char *json_data,
                               mongoc_collection_t *c, const char **line_nos,
                               int line_no_cnt)
{
    mongoc_cursor_t *cursor = cross_platform_mongoc_collection_find(c, query);
    bson_error_t error;
    int i;

    bson_t *doc;
    bson_t *bson = bson_new_from_json((const uint8_t *)json_data, -1, &error);
    if (!bson) {
        fprintf(stderr, "%s\n", error.message);
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        return EXIT_FAILURE;
    }
    if (!mongoc_cursor_next(cursor, &doc)) {
        lol_debug("insert new record to db\n");
        if (!cross_platform_mongoc_collection_insert(c, bson, &error)) {
            fprintf(stderr, "%s\n", error.message);
        }
    } else {
        bson_t *update = bson_new();

        bson_iter_t iter;
        bson_iter_init_find(&iter, doc, "line_name");
        if (BSON_ITER_HOLDS_ARRAY(&iter)) {
            bool update_db = false;
            for (i = 0; i < line_no_cnt; i++) {
                int append = 1;
                bson_iter_t arr_iter;
                bson_iter_recurse(&iter, &arr_iter);
                while (bson_iter_next(&arr_iter)) {
                    if (BSON_ITER_HOLDS_UTF8(&arr_iter)) {
                        const char *value;
                        uint32_t len;
                        value = bson_iter_utf8(&arr_iter, &len);
                        if (strcmp(value, line_nos[i]) == 0) {
                            append = 0;
                            break;
                        }
                    }
                }
                if (append) {
                    bson_append_utf8(update, "line_name", -1, line_nos[i], -1);
                    update_db = true;
                }
            }
            /* try not to update the database as much as possible */
            if (update_db) {
                lol_debug("update record for db\n");
                bson_t *push_update = BCON_NEW("$push", BCON_DOCUMENT(update));
                update_db_by_query(query, push_update, c, &error);
                bson_destroy(push_update);
            }
        }
        bson_destroy(update);
    }

    bson_destroy(bson);
    bson_destroy(doc);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
}

int update_line_info(bson_t *query, mongoc_collection_t *c, const char **line_nos, int line_no_cnt)
{
    bson_error_t error;
    bson_t *line_array = bson_new();
    for (int i = 0; i < line_no_cnt; i++) {
        char str[10];
        sprintf(str, "%d", i);
        BSON_APPEND_UTF8(line_array, str, line_nos[i]);
    }

    bson_t *update = BCON_NEW("$addToSet", "{", "line_no", "{", "$each", BCON_ARRAY(line_array), "}", "}");
    bson_t *opts = BCON_NEW("upsert", BCON_BOOL(true));
    mongoc_collection_update_one(c, query, update, opts, NULL, &error);

    bson_destroy(opts);
    bson_destroy(update);
    bson_destroy(line_array);
    bson_destroy(query);
}

MongoDriver::MongoDriver(const char *uri)
{
    if (uri && strlen(uri) > 0) {
        memcpy(db_uri, uri, strlen(uri) + 1);
    } else {
        const char *t = "mongodb://localhost/cdrs_db";
        memcpy(db_uri, t, strlen(t) + 1);
    }
    init_dbi(db_uri);
}

MongoDriver::~MongoDriver()
{
    destroy_dbi();
}

static char *masked_db_uri(const char *db_uri)
{
    char *tmp;
    char *array[2], *saveptr = NULL;
    char *masked = NULL;

    tmp = strdup(db_uri);

    memset(array, 0, sizeof(array));
    array[0] = strtok_r(tmp, "@", &saveptr);
    if (array[0]) array[1] = strtok_r(NULL, "@", &saveptr);

    if (array[1]) {
        masked = (char *)malloc(strlen(array[1])
                                + strlen("mongodb://*****:*****@") + 1);
        if (masked) {
            sprintf(masked, "mongodb://*****:*****@%s", array[1]);
        }
    } else {
        masked = strdup(array[0]);
    }

    free(tmp);

    return masked;
}

/*
 * We've added it
 * Because the following function is deprecated in the mongo-c-driver
 */
static bool mongoc_client_get_server_status(mongoc_client_t *client, /* IN */
                                     mongoc_read_prefs_t *read_prefs, /* IN */
                                     bson_t *reply, /* OUT */
                                     bson_error_t *error) /* OUT */
{
    bson_t cmd = BSON_INITIALIZER;
    bool ret = false;

    BSON_ASSERT(client);

    BSON_APPEND_INT32(&cmd, "serverStatus", 1);
    ret = mongoc_client_command_simple(client, "admin", &cmd, read_prefs, reply,
                                       error);
    bson_destroy(&cmd);

    return ret;
}

int MongoDriver::init_mongoc(const char *db_uri)
{
    bson_t reply;
    bson_error_t error;
    bson_iter_t iter;

    const mongoc_uri_t *uri;

    if (!db_uri) {
        return -1;
    }

    memset(&self, 0, sizeof(mongoc_t));
    self.masked_db_uri = masked_db_uri(db_uri);

    mongoc_init();
    self.initialized = true;
    self.client = mongoc_client_new(db_uri);
    if (!self.client) {
        return -1;
    }

#if MONGOC_MAJOR_VERSION >= 1 && MONGOC_MINOR_VERSION >= 4
    mongoc_client_set_error_api((mongoc_client_t *)self.client, 2);
#endif

    uri = mongoc_client_get_uri((mongoc_client_t *)self.client);
    if (!uri) {
        return -1;
    }

    self.name = mongoc_uri_get_database(uri);
    if (!self.name) {
        return -1;
    }

    self.database =
        mongoc_client_get_database((mongoc_client_t *)self.client, self.name);
    if (!self.database) {
        return -1;
    }

    if (!mongoc_client_get_server_status((mongoc_client_t *)self.client, NULL,
                                         &reply, &error)) {
        printf("Failed to connect to server [%s]. Cause: %s\n",
               self.masked_db_uri, error.message);
        return -1;
    }

    if (!bson_iter_init_find(&iter, &reply, "ok")) {
        return -1;
    }

    bson_destroy(&reply);

    printf("MongoDB URI: '%s'\n", self.masked_db_uri);

    return 0;
}

int MongoDriver::init_dbi(const char *db_uri)
{
    int rv;

    rv = init_mongoc(db_uri);
    if (rv != 0) return rv;

    if (self.client && self.name) {
        self.collection.gtpc = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "gtpc_cdr");
        if (!self.collection.gtpc) {
            return -1;
        }
        self.collection.gtpu = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "gtpu_cdr");
        if (!self.collection.gtpu) {
            return -1;
        }
        self.collection.diameter = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "diameter_cdr");
        if (!self.collection.diameter) {
            return -1;
        }
        self.collection.map = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "map_cap_cdr");
        if (!self.collection.map) {
            return -1;
        }
        self.collection.http2 = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "http2_cdr");
        if (!self.collection.http2) {
            return -1;
        }
        self.collection.s1ap = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "s1ap_tdr");
        if (!self.collection.s1ap) {
            return -1;
        }
        self.collection.ngap = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "ngap_tdr");
        if (!self.collection.ngap) {
            return -1;
        }
        self.collection.h323 = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "h323_cdr");
        if (!self.collection.h323) {
            return -1;
        }
        self.collection.h248 = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "h248_tdr");
        if (!self.collection.h248) {
            return -1;
        }
        self.collection.info_vault = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "info_vault");
        if (!self.collection.info_vault) {
            return -1;
        }
        self.collection.line_info = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "line_info");
        if (!self.collection.line_info) {
            return -1;
        }
        self.collection.exception = mongoc_client_get_collection(
            (mongoc_client_t *)self.client, self.name, "abnormal_cdr");
        if (!self.collection.exception) {
            return -1;
        }
    }

    return 0;
}

void MongoDriver::destroy_dbi()
{
    if (self.collection.gtpc) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.gtpc);
    }
    if (self.collection.gtpu) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.gtpu);
    }
    if (self.collection.diameter) {
        mongoc_collection_destroy(
            (mongoc_collection_t *)self.collection.diameter);
    }
    if (self.collection.map) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.map);
    }
    if (self.collection.http2) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.http2);
    }
    if (self.collection.s1ap) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.s1ap);
    }
    if (self.collection.ngap) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.ngap);
    }
    if (self.collection.h323) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.h323);
    }
    if (self.collection.h248) {
        mongoc_collection_destroy((mongoc_collection_t *)self.collection.h248);
    }
    if (self.collection.info_vault) {
        mongoc_collection_destroy(
            (mongoc_collection_t *)self.collection.info_vault);
    }
    if (self.collection.line_info) {
        mongoc_collection_destroy(
            (mongoc_collection_t *)self.collection.line_info);
    }
    if (self.collection.exception) {
        mongoc_collection_destroy(
            (mongoc_collection_t *)self.collection.exception);
    }

    destroy_mongoc();
}

void MongoDriver::destroy_mongoc()
{
    if (self.database) {
        mongoc_database_destroy((mongoc_database_t *)self.database);
        self.database = NULL;
    }
    if (self.client) {
        mongoc_client_destroy((mongoc_client_t *)self.client);
        self.client = NULL;
    }
    if (self.masked_db_uri) {
        free(self.masked_db_uri);
        self.masked_db_uri = NULL;
    }
    if (self.initialized) {
        mongoc_cleanup();
        self.initialized = false;
    }
}

/* compile alone: g++ dbi.cpp $(pkg-config --libs --cflags libmongoc-1.0) */
