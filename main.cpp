#include "dbi.h"
#include <sstream>
#include <string>
#include <iostream>

std::string get_rand_json_string()
{
    std::ostringstream oss;
    oss << "{";
    oss << "\"AAA\": " << rand() << ", ";
    oss << "\"BBB\": " << rand() << ", ";
    oss << "\"CCC\": " << rand() << ", ";
    oss << "\"DDD\": " << rand() << ", ";
    oss << "\"EEE\": " << rand();
    oss << "}";
    return oss.str();
}

std::string create_query_doc(int key)
{
    std::ostringstream oss;
    oss << "{";
    oss << "\"key\": " << key;
    oss << "}";
    return oss.str();
}

std::string get_rand_json_string2(int key)
{
    std::ostringstream oss;
    oss << "{";
    oss << "\"key\": " << key << ", ";
    oss << "\"AAA\": " << rand() << ", ";
    oss << "\"BBB\": " << rand() << ", ";
    oss << "\"CCC\": " << rand() << ", ";
    oss << "\"DDD\": " << rand() << ", ";
    oss << "\"EEE\": " << rand();
    oss << "}";
    return oss.str();
}

std::string get_rand_json_string3(int key)
{
    std::ostringstream oss;
    oss << "{";
    oss << "\"key\": " << key << ", ";
    oss << "\"AAA\": " << rand() << ", ";
    oss << "\"BBB\": " << rand() << ", ";
    oss << "\"CCC\": " << rand() << ", ";
    oss << "\"line_name\": [" << (rand()+1000) << "]";
    oss << "}";
    return oss.str();
}

#define EXEC_CNT 10000
int main()
{
    MongoDriver *driver = new MongoDriver("mongodb://localhost/test");
    bson_t **docs = (bson_t **)malloc(sizeof(bson_t *) * EXEC_CNT);

    // 使用当前时间作为随机数种子
    srand(time(0));
    // 记录程序开始时间
    time_t start_time;
    time(&start_time);

#ifdef ENABLE_INSERT_TEST
    for (int i = 0; i < EXEC_CNT; i++) {
        insert_data_directly(get_rand_json_string().c_str(), driver->get_mongoc()->collection.gtpc);
    }
    // for (int i = 0; i < EXEC_CNT; i++) {
    //     docs[i] = bson_new_from_json((const uint8_t *)get_rand_json_string().c_str(), -1, NULL);
    // }
    // insert_many_docs_directly(docs, EXEC_CNT, driver->get_mongoc()->collection.gtpc);
#endif

#ifdef ENABLE_UPDATE_TEST
    // for (int i = 0; i < EXEC_CNT; i++) {
    //     int key = rand() % 10000;
    //     bson_t *query = bson_new_from_json((const uint8_t *)create_query_doc(key).c_str(), -1, NULL);
    //     update_or_insert_data_by_query(query, get_rand_json_string2(key).c_str(), driver->get_mongoc()->collection.gtpc);
    // }

    /* exec 10000 times need 15s */
    for (int i = 0; i < EXEC_CNT; i++) {
        int key = rand() % 10000;
        bson_t *query = bson_new_from_json((const uint8_t *)create_query_doc(key).c_str(), -1, NULL);
        update_data_by_query(query, get_rand_json_string2(key).c_str(), driver->get_mongoc()->collection.gtpc);
    }

    /* exec 10000 times need 17.5s */
    // mongoc_bulk_operation_t *bulk = mongoc_collection_create_bulk_operation_with_opts(driver->get_mongoc()->collection.gtpc, NULL);
    // for (int i = 0; i < EXEC_CNT; i++) {
    //     int key = rand() % 10000;
    //     bson_t *query = bson_new_from_json((const uint8_t *)create_query_doc(key).c_str(), -1, NULL);
    //     bson_t *doc = bson_new_from_json((const uint8_t *)get_rand_json_string2(key).c_str(), -1, NULL);
    //     bson_t *update = BCON_NEW("$set", BCON_DOCUMENT(doc));
    //     mongoc_bulk_operation_update(bulk, query, update, true);
    //     bson_destroy(update);
    //     bson_destroy(doc);
    //     bson_destroy(query);
    // }
    // int ret = mongoc_bulk_operation_execute(bulk, NULL, NULL);
    // if (!ret) {
    //     fprintf(stderr, "error buck operation execute\n");
    // }
    // mongoc_bulk_operation_destroy(bulk);
#endif

#ifdef ENABLE_ARRAY_TEST
//     for (int i = 0; i < EXEC_CNT; i++) {
//         int key = rand() % 10000;
//         bson_t *query = bson_new_from_json((const uint8_t *)create_query_doc(key).c_str(), -1, NULL);
// #define LINE_NO_CNT 1
//         char **line_nos = (char **)malloc(sizeof(char *) * LINE_NO_CNT);
//         for (int i = 0; i < LINE_NO_CNT; i++) {
//             line_nos[i] = "dal";
//         }
//         update_or_insert_line_info(query, get_rand_json_string3(key).c_str(), driver->get_mongoc()->collection.gtpc,
//                                    line_nos, LINE_NO_CNT);
//         free(line_nos);
//     }

    for (int i = 0; i < EXEC_CNT; i++) {
        int key = rand() % 10000;
        bson_t *query = bson_new_from_json((const uint8_t *)create_query_doc(key).c_str(), -1, NULL);
#define LINE_NO_CNT 1
        char **line_nos = (char **)malloc(sizeof(char *) * LINE_NO_CNT);
        for (int i = 0; i < LINE_NO_CNT; i++) {
            // line_nos[i] = (char *)std::to_string(rand()).c_str();
            line_nos[i] = "";
        }
        update_line_info(query, driver->get_mongoc()->collection.gtpc,
                         line_nos, LINE_NO_CNT);
        free(line_nos);
    }
#endif

    // 记录程序结束时间
    time_t end_time;
    time(&end_time);

    free(docs);
    // 计算程序运行时间
    double execution_time = difftime(end_time, start_time);

    // 打印程序运行时间
    printf("程序开始时间：%s", ctime(&start_time));
    printf("程序结束时间：%s", ctime(&end_time));
    printf("程序运行时间：%.2f 秒\n", execution_time);

    return 0;
}