export const CONFIG = {
    "schema": null,
    "codecs": "snappy",
    "keepFields": ['id', 'first_name', 'last_name', 'email'],
    "ignoreFields": null,
};

export const setConfig = (newConfig) => {
    Object.keys(newConfig).forEach((key) => {
        CONFIG[key] = newConfig[key];
    });
}
