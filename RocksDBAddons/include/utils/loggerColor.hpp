#pragma once

// the following are UBUNTU/LINUX ONLY terminal color codes.
#define RESET "\033[0m"
#define BLACK "\033[30m" /* Black */
#define RED "\033[31m" /* Red */
#define GREEN "\033[32m" /* Green */
#define YELLOW "\033[33m" /* Yellow */
#define BLUE "\033[34m" /* Blue */
#define MAGENTA "\033[35m" /* Magenta */
#define CYAN "\033[36m" /* Cyan */
#define WHITE "\033[37m" /* White */
#define BOLDBLACK "\033[1m\033[30m" /* Bold Black */
#define BOLDRED "\033[1m\033[31m" /* Bold Red */
#define BOLDGREEN "\033[1m\033[32m" /* Bold Green */
#define BOLDYELLOW "\033[1m\033[33m" /* Bold Yellow */
#define BOLDBLUE "\033[1m\033[34m" /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m" /* Bold Magenta */
#define BOLDCYAN "\033[1m\033[36m" /* Bold Cyan */
#define BOLDWHITE "\033[1m\033[37m" /* Bold White */

static std::string _CutParenthesesNTail(std::string&& prettyFuncon)
{
    auto pos = prettyFuncon.find('(');
    if (pos != std::string::npos) {
        prettyFuncon.erase(prettyFuncon.begin() + pos, prettyFuncon.end());
    }
    auto posSpace = prettyFuncon.find(' ');
    if (posSpace != std::string::npos) {
        prettyFuncon.erase(prettyFuncon.begin(), prettyFuncon.begin() + posSpace + 1);
    }
    if (prettyFuncon.find("DELTAKV_NAMESPACE::") != std::string::npos) {
        prettyFuncon.erase(prettyFuncon.begin(), prettyFuncon.begin() + 19);
    }
    return std::move(prettyFuncon);
}

static std::string _CutFileNameTail(std::string&& prettyFuncon)
{
    while (true) {
        auto pos = prettyFuncon.find('/');
        if (pos != std::string::npos) {
            prettyFuncon.erase(prettyFuncon.begin(), prettyFuncon.begin() + pos + 1);
        } else {
            break;
        }
    }
    return std::move(prettyFuncon);
}

// means function name
#define __STR_FUNCTION__ _CutParenthesesNTail(std::string(__PRETTY_FUNCTION__))
// means function name + parentheses (P = parentheses)
#define __STR_FUNCTIONP__ __STR_FUNCTION__ + "()"
// means function name + parentheses + colon (C = colon)
#define __STR_FUNCTIONPC__ __STR_FUNCTION__ + "(): "

#define __STR_FILE__ _CutFileNameTail(std::string(__FILE__))