/**
 * Copyright (C) 2012-2015 Yecheng Fu <cofyc.jackson at gmail dot com>
 * All rights reserved.
 *
 * Use of this source code is governed by a MIT-style license that can be found
 * in the LICENSE file.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include "argparse.h"

#define OPT_UNSET 1
#define OPT_LONG  (1 << 1)

static const wchar_t*
prefix_skip(const wchar_t* str, const wchar_t* prefix)
{
    size_t len = wcslen(prefix);
    return wcsncmp(str, prefix, len) ? NULL : str + len;
}

static int
prefix_cmp(const wchar_t* str, const wchar_t* prefix)
{
    for (;; str++, prefix++)
        if (!*prefix) {
            return 0;
        }
        else if (*str != *prefix) {
            return (wchar_t)*prefix - (wchar_t)*str;
        }
}

static void
argparse_error(struct argparse* self, const struct argparse_option* opt,
    const wchar_t* reason, int flags)
{
    (void)self;
    if (flags & OPT_LONG) {
        fwprintf(stderr, L"error: option `--%s` %s\n", opt->long_name, reason);
    }
    else {
        fwprintf(stderr, L"error: option `-%c` %s\n", opt->short_name, reason);
    }
    exit(EXIT_FAILURE);
}

static int
argparse_getvalue(struct argparse* self, const struct argparse_option* opt,
    int flags)
{
    const wchar_t* s = NULL;
    if (!opt->value)
        goto skipped;
    switch (opt->type) {
    case ARGPARSE_OPT_BOOLEAN:
        if (flags & OPT_UNSET) {
            *(int*)opt->value = *(int*)opt->value - 1;
        }
        else {
            *(int*)opt->value = *(int*)opt->value + 1;
        }
        if (*(int*)opt->value < 0) {
            *(int*)opt->value = 0;
        }
        break;
    case ARGPARSE_OPT_BIT:
        if (flags & OPT_UNSET) {
            *(int*)opt->value &= ~opt->data;
        }
        else {
            *(int*)opt->value |= opt->data;
        }
        break;
    case ARGPARSE_OPT_STRING:
        if (self->optvalue) {
            *(const wchar_t**)opt->value = self->optvalue;
            self->optvalue = NULL;
        }
        else if (self->argc > 1) {
            self->argc--;
            *(const wchar_t**)opt->value = *++self->argv;
        }
        else {
            argparse_error(self, opt, L"requires a value", flags);
        }
        break;
    case ARGPARSE_OPT_INTEGER:
        errno = 0;
        if (self->optvalue) {
            *(int*)opt->value = wcstol(self->optvalue, (wchar_t**)&s, 0);
            self->optvalue = NULL;
        }
        else if (self->argc > 1) {
            self->argc--;
            *(int*)opt->value = wcstol(*++self->argv, (wchar_t**)&s, 0);
        }
        else {
            argparse_error(self, opt, L"requires a value", flags);
        }
        if (errno == ERANGE)
            argparse_error(self, opt, L"numerical result out of range", flags);
        if (s == NULL) 
            argparse_error(self, opt, L"endptr is NULL", flags);
        if (s[0] != L'\0') // no digits or contains invalid characters
            argparse_error(self, opt, L"expects an integer value", flags);
        break;
    case ARGPARSE_OPT_FLOAT:
        errno = 0;
        if (self->optvalue) {
            *(float*)opt->value = wcstof(self->optvalue, (wchar_t**)&s);
            self->optvalue = NULL;
        }
        else if (self->argc > 1) {
            self->argc--;
            *(float*)opt->value = wcstof(*++self->argv, (wchar_t**)&s);
        }
        else {
            argparse_error(self, opt, L"requires a value", flags);
        }
        if (errno == ERANGE)
            argparse_error(self, opt, L"numerical result out of range", flags);
        if (s == NULL)
            argparse_error(self, opt, L"endptr is NULL", flags);
        if (s[0] != L'\0') // no digits or contains invalid characters
            argparse_error(self, opt, L"expects a numerical value", flags);
        break;
    default:
        assert(0);
    }

skipped:
    if (opt->callback) {
        return opt->callback(self, opt);
    }
    return 0;
}

static void
argparse_options_check(const struct argparse_option* options)
{
    for (; options->type != ARGPARSE_OPT_END; options++) {
        switch (options->type) {
        case ARGPARSE_OPT_END:
        case ARGPARSE_OPT_BOOLEAN:
        case ARGPARSE_OPT_BIT:
        case ARGPARSE_OPT_INTEGER:
        case ARGPARSE_OPT_FLOAT:
        case ARGPARSE_OPT_STRING:
        case ARGPARSE_OPT_GROUP:
            continue;
        default:
            fwprintf(stderr, L"wrong option type: %d", options->type);
            break;
        }
    }
}

static int
argparse_short_opt(struct argparse* self, const struct argparse_option* options)
{
    for (; options->type != ARGPARSE_OPT_END; options++) {
        if (options->short_name == *self->optvalue) {
            self->optvalue = self->optvalue[1] ? self->optvalue + 1 : NULL;
            return argparse_getvalue(self, options, 0);
        }
    }
    return -2;
}

static int
argparse_long_opt(struct argparse* self, const struct argparse_option* options)
{
    for (; options->type != ARGPARSE_OPT_END; options++) {
        const wchar_t* rest;
        int opt_flags = 0;
        if (!options->long_name)
            continue;

        rest = prefix_skip(self->argv[0] + 2, options->long_name);
        if (!rest) {
            // negation disabled?
            if (options->flags & OPT_NONEG) {
                continue;
            }
            // only OPT_BOOLEAN/OPT_BIT supports negation
            if (options->type != ARGPARSE_OPT_BOOLEAN && options->type !=
                ARGPARSE_OPT_BIT) {
                continue;
            }

            if (prefix_cmp(self->argv[0] + 2, L"no-")) {
                continue;
            }
            rest = prefix_skip(self->argv[0] + 2 + 3, options->long_name);
            if (!rest)
                continue;
            opt_flags |= OPT_UNSET;
        }
        if (*rest) {
            if (*rest != L'=' )
                continue;
            self->optvalue = rest + 1;
        }
        return argparse_getvalue(self, options, opt_flags | OPT_LONG);
    }
    return -2;
}

int
argparse_init(struct argparse* self, struct argparse_option* options,
    const wchar_t* const* usages, int flags)
{
    memset(self, 0, sizeof(*self));
    self->options = options;
    self->usages = usages;
    self->flags = flags;
    self->description = NULL;
    self->epilog = NULL;
    return 0;
}

void
argparse_describe(struct argparse* self, const wchar_t* description,
    const wchar_t* epilog)
{
    self->description = description;
    self->epilog = epilog;
}

int
argparse_parse(struct argparse* self, int argc, const wchar_t** argv)
{
    self->argc = argc - 1;
    self->argv = argv + 1;
    self->out = argv;

    argparse_options_check(self->options);

    for (; self->argc; self->argc--, self->argv++) {
        const wchar_t* arg = self->argv[0];
        if (arg[0] != L'-' || !arg[1]) {
            if (self->flags & ARGPARSE_STOP_AT_NON_OPTION) {
                goto end;
            }
            // if it's not option or is a single char '-', copy verbatim
            self->out[self->cpidx++] = self->argv[0];
            continue;
        }
        // short option
        if (arg[1] != L'-') {
            self->optvalue = arg + 1;
            switch (argparse_short_opt(self, self->options)) {
            case -1:
                break;
            case -2:
                goto unknown;
            }
            while (self->optvalue) {
                switch (argparse_short_opt(self, self->options)) {
                case -1:
                    break;
                case -2:
                    goto unknown;
                }
            }
            continue;
        }
        // if '--' presents
        if (!arg[2]) {
            self->argc--;
            self->argv++;
            break;
        }
        // long option
        switch (argparse_long_opt(self, self->options)) {
        case -1:
            break;
        case -2:
            goto unknown;
        }
        continue;

    unknown:
        fwprintf(stderr, L"error: unknown option `%s`\n", self->argv[0]);
        argparse_usage(self);
        if (!(self->flags & ARGPARSE_IGNORE_UNKNOWN_ARGS)) {
            exit(EXIT_FAILURE);
        }
    }

end:
    memmove(self->out + self->cpidx, self->argv, self->argc * sizeof(*self->out));
    self->out[self->cpidx + self->argc] = NULL;

    return self->cpidx + self->argc;
}

void
argparse_usage(struct argparse* self)
{
    if (self->usages) {
        fwprintf(stdout, L"Usage: %s\n", *self->usages++);
        while (*self->usages && **self->usages)
            fwprintf(stdout, L"   or: %s\n", *self->usages++);
    }
    else {
        fwprintf(stdout, L"Usage:\n");
    }

    // print description
    if (self->description)
        fwprintf(stdout, L"%s\n", self->description);

    fputc('\n', stdout);

    const struct argparse_option* options;

    // figure out best width
    size_t usage_opts_width = 0;
    size_t len;
    options = self->options;
    for (; options->type != ARGPARSE_OPT_END; options++) {
        len = 0;
        if ((options)->short_name) {
            len += 2;
        }
        if ((options)->short_name && (options)->long_name) {
            len += 2;           // separator ", "
        }
        if ((options)->long_name) {
            len += wcslen((options)->long_name) + 2;
        }
        if (options->type == ARGPARSE_OPT_INTEGER) {
            len += wcslen(L"=<int>");
        }
        if (options->type == ARGPARSE_OPT_FLOAT) {
            len += wcslen(L"=<flt>");
        }
        else if (options->type == ARGPARSE_OPT_STRING) {
            len += wcslen(L"=<str>");
        }
        len = (len + 3) - ((len + 3) & 3);
        if (usage_opts_width < len) {
            usage_opts_width = len;
        }
    }
    usage_opts_width += 4;      // 4 spaces prefix

    options = self->options;
    for (; options->type != ARGPARSE_OPT_END; options++) {
        size_t pos = 0;
        size_t pad = 0;
        if (options->type == ARGPARSE_OPT_GROUP) {
            fputc('\n', stdout);
            fwprintf(stdout, L"%s", options->help);
            fputc('\n', stdout);
            continue;
        }
        pos = fwprintf(stdout, L"    ");
        if (options->short_name) {
            pos += fwprintf(stdout, L"-%c", options->short_name);
        }
        if (options->long_name && options->short_name) {
            pos += fwprintf(stdout, L", ");
        }
        if (options->long_name) {
            pos += fwprintf(stdout, L"--%s", options->long_name);
        }
        if (options->type == ARGPARSE_OPT_INTEGER) {
            pos += fwprintf(stdout, L"=<int>");
        }
        else if (options->type == ARGPARSE_OPT_FLOAT) {
            pos += fwprintf(stdout, L"=<flt>");
        }
        else if (options->type == ARGPARSE_OPT_STRING) {
            pos += fwprintf(stdout, L"=<str>");
        }
        if (pos <= usage_opts_width) {
            pad = usage_opts_width - pos;
        }
        else {
            fputc('\n', stdout);
            pad = usage_opts_width;
        }
        fwprintf(stdout, L"%*s%s\n", (int)pad + 2, L"", options->help);
    }

    // print epilog
    if (self->epilog)
        fwprintf(stdout, L"%s\n", self->epilog);
}

int
argparse_help_cb_no_exit(struct argparse* self,
    const struct argparse_option* option)
{
    (void)option;
    argparse_usage(self);
    return (EXIT_SUCCESS);
}

int
argparse_help_cb(struct argparse* self, const struct argparse_option* option)
{
    argparse_help_cb_no_exit(self, option);
    exit(EXIT_SUCCESS);
}

