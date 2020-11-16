#!/usr/bin/env python

import os
import sys

from lib.iplugin import TxPlugin
from core.globals import G_LOGGER
from importlib.util import spec_from_file_location, module_from_spec


class PluginManager:
    """插件管理器基类，子类须实现load_plugins
    """
    name = "base"

    def __init__(self, plugins=()):
        self.__plugins = []
        if plugins:
            self.add_plugins(plugins)

    def __iter__(self):
        return iter(self.plugins)

    def add_plugin(self, plug):
        """
        添加插件
        :param plug:
        :return:
        """
        self.__plugins.append(plug)

    def add_plugins(self, plugins):
        """
        添加多个插件
        :param plugins:
        :return:
        """
        for plug in plugins:
            self.add_plugin(plug)

    def del_plugin(self, plug):
        """
        删除插件
        :param plug:
        :return:
        """
        if plug in self.__plugins:
            self.__plugins.remove(plug)

    def del_plugins(self, plugins):
        """
        删除多个插件
        :param plugins:
        :return:
        """
        for plug in plugins:
            self.del_plugin(plug)

    def get_plugins(self, name=None):
        """
        获取插件
        :param name: 插件名称，为None返回所有插件
        :return:
        """
        plugins = []
        for plugin in self.__plugins:
            if name is None or plugin.name == name:
                plugins.append(plugin)
        return plugins[0] if len(plugins) == 1 else plugins

    def _load_plugin(self, plug):
        loaded = False
        for p in self.plugins:
            if p.name == plug.name:
                loaded = True
                break
        if not loaded:
            self.add_plugin(plug)

    def load_plugins(self):
        raise NotImplementedError("must implement load_plugins function")

    def _get_plugins(self):
        return self.__plugins

    def _set_plugins(self, plugins):
        self.__plugins = []
        self.add_plugins(plugins)

    plugins = property(_get_plugins, _set_plugins, None, """访问插件管理器中的插件列表""")


class DirectoryPluginManager(PluginManager):
    """目录插件管理器
    """
    name = "directory"

    def __init__(self, plugins=(), config={}):
        super().__init__(plugins)
        default_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), "coin")
        self.directory = config.get("directory", default_directory)

    def _read_dir(self, plugin_list, directory):
        """
        递归遍历插件目录
        :return:
        """
        try:
            for f in os.listdir(directory):
                sub_path = os.path.join(directory, f)
                if os.path.isdir(sub_path):
                    self._read_dir(plugin_list, sub_path)
                else:
                    if f.endswith(".py") and f != "__init__.py":
                        plugin_list.append((f[:-3], sub_path))
        except OSError:
            G_LOGGER.error("Failed to access: %s" % directory)
        return plugin_list

    def load_plugins(self, coin_name):
        """
        实现加载插件的方法
        :return:
        """
        plugin_list = self._read_dir([], self.directory)
        for (name, path) in plugin_list:
            # 只加载当前币种对应的插件
            if name != coin_name:
                continue
            module_spec = spec_from_file_location(name, path)
            if module_spec:
                old = sys.modules.get(name)
                if old is not None:
                    del sys.modules[name]
                module = module_from_spec(module_spec)
                module_spec.loader.exec_module(module)
                if hasattr(module, "__all__"):
                    attrs = [getattr(module, x) for x in module.__all__]
                    for plug in attrs:
                        if not issubclass(plug, TxPlugin):
                            continue
                        self._load_plugin(plug())
