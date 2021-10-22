#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#include <stdint.h>

// This implementation only supports null-termianted keys.
// The reason is that Redis represents keys as a pointer to the key data (null
// terminated) and the key length is stored separately (the length is required
// to allow for keys with null bytes). We'd like to be able to directly search
// Redis keys in the tree, without first converting them to some other
// representation (e.g. a struct containing length and pointer fields), which
// would require a memory allocation. Therefore our only option is to give the
// find function, which expects key_type&, a pointer to the key data, and rely
// on the terminating null to deduce the key size.

typedef struct {
	double value;
	uint8_t key[];
} stx_kv_t;

EXTERNC void* stx_create();
EXTERNC int stx_insert(void* tree, stx_kv_t* new_kv);
EXTERNC stx_kv_t* stx_lookup(void* tree, uint8_t* key);
EXTERNC void* stx_iter_create();
EXTERNC void stx_iter_goto(void* tree, void* iter, uint8_t* key);
EXTERNC stx_kv_t* stx_iter_next(void* tree, void* iter);
EXTERNC void stx_iter_free(void* iter);
EXTERNC void stx_free(void* tree);