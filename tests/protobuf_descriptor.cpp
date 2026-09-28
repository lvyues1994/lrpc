#include "echo.rpc.hpp"

rpc::service_descriptor const *descriptor_from_other_translation_unit() {
    return &demo::Echo_service_descriptor();
}
