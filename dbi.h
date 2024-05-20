#ifndef _DBI_H
#define _DBI_H

#include <mongoc.h>

typedef struct ogs_mongoc_s
{
    bool initialized;
    const char *name;
    void *uri;
    void *client;
    void *database;

    char *masked_db_uri;

    struct
    {
        void *gtpc;
        void *gtpu;
        void *diameter;
        void *map;
        void *http2;
        void *ngap;
        void *s1ap;
        void *h323;
        void *h248;

        void *info_vault;
        void *line_info;
        void *exception;
    } collection;
} mongoc_t;

#define GET_COLLECTION_BY_NAME(m, name) \
(mongoc_collection_t *)m->collection.name

/* API */
int update_or_insert_data_by_query(bson_t *query, const char *json_data, mongoc_collection_t *c);
int update_or_insert_line_info(bson_t *query, const char *json_data, mongoc_collection_t *c, const char **line_nos, int line_no_cnt);
int insert_data_directly(const char *json_data, mongoc_collection_t *c);
int insert_many_docs_directly(bson_t **docs, size_t size, mongoc_collection_t *c);

class MongoDriver final
{
public:
    MongoDriver(MongoDriver const &) = delete;
    MongoDriver &operator=(MongoDriver const &) = delete;
    MongoDriver(const char *db_uri);
    ~MongoDriver();

    mongoc_t *get_mongoc()
    {
        return &self;
    }

private:
    mongoc_t self;
    char db_uri[64];

private:
    int init_mongoc(const char *db_uri);
    int init_dbi(const char *db_uri);
    void destroy_dbi();
    void destroy_mongoc();
};

#endif