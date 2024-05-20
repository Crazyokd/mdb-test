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

#define EXEC_CNT 1000
int main()
{
    printf("hello world\n");

    MongoDriver *driver = new MongoDriver("mongodb://localhost/test");
    bson_t **docs = (bson_t **)malloc(sizeof(bson_t *) * EXEC_CNT);

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
    for (int i = 0; i < EXEC_CNT; i++) {
        update_or_insert_data_by_query(bson_new(), get_rand_json_string().c_str(), driver->get_mongoc()->collection.gtpc);
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