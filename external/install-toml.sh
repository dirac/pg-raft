#!/bin/bash

source "$(dirname -- "$0")/install-functions.sh"

VERSION=$TOML_VERSION
SOURCE=SOURCES/toml-${VERSION}.tar.gz

download https://github.com/ToruNiina/toml11/archive/refs/tags/v${VERSION}.tar.gz ${SOURCE}

cd BUILDS && rm -rf toml11-${VERSION}
exit_on_error $? ${LINENO} "cd BUILDS"

gunzip -d < ../${SOURCE} | tar xvf -
exit_on_error $? ${LINENO} "gunzip -d < ../${SOURCE} | tar xvf -"

cd toml11-${VERSION}
exit_on_error $? ${LINENO} "cd toml11-${VERSION}"

cp single_include/toml.hpp ${prefix}/include/.
exit_on_error $? ${LINENO} "cp single_include/toml.hpp ${prefix}/include/."

cd ${DIR}
exit_on_error $? ${LINENO} 'cd ${DIR}'

rm -r BUILDS/toml11-${VERSION}
exit_on_error $? ${LINENO} 'rm -r BUILDS/toml11-${VERSION}'

##### Usage from README.
#
# #include <toml.hpp>
# #include <iostream>
# 
# // ```toml
# // title = "an example toml file"
# // nums  = [3, 1, 4, 1, 5] # pi!
# // ```
# 
# int main()
# {
#     // select TOML version at runtime (optional)
#     auto data = toml::parse("example.toml", toml::spec::v(1,1,0));
# 
#     // find a value with the specified type from a table
#     std::string title = toml::find<std::string>(data, "title");
# 
#     // convert the whole array into STL-like container automatically
#     std::vector<int> nums = toml::find<std::vector<int>>(data, "nums");
# 
#     // access with STL-like manner
#     if( ! data.contains("foo"))
#     {
#         data["foo"] = "bar";
#     }
#     if(data.at("nums").is_array())
#     {
#         data.push_back(9);
#     }
# 
#     // check comments
#     assert(data.at("nums").comments().at(0) == "# pi!");
# 
#     // pass a fallback
#     std::string name = toml::find_or<std::string>(data, "name", "not found");
# 
#     // serialization considering format info
#     data.at("nums").as_array_fmt().fmt = toml::array_format::multiline;
#     data.at("nums").as_array_fmt().indent_type = toml::indent_char::space;
#     data.at("nums").as_array_fmt().body_indent = 2;
# 
#     std::cout << toml::format(data) << std::endl;
# 
#     return 0;
# }
#
