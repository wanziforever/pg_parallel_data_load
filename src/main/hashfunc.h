#include "hgint.h"

int get_matching_hash_bounds_int(int var, int greatest_modulus);
int get_matching_hash_bounds_bigint(int64 var, int greatest_modulus);
int get_matching_hash_bounds_string(char* var, int greatest_modulus);
int get_matching_hash_bounds_numeric(char *var, int greatest_modulus);
