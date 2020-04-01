#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/19 15:17
# @Author  : xuepeng
# @Email   : xue.peng@datatom.com

import os

from configparser import ConfigParser
import subprocess
import logging
import time
import sys

from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED

CONF = '/etc/my.cnf'

# 创建一个logger日志器
logger = logging.getLogger("galera_manager")
# 设置日志级别
logger.setLevel(logging.DEBUG)
# 设置日志的处理器handler为console流,即在屏幕输出
handler = logging.StreamHandler(sys.stdout)
# 设置日志格式
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)s %(message)s")
handler.setFormatter(formatter)
# 向日志器添加处理器
logger.addHandler(handler)


class TimeoutError(Exception):
    """Timeout error Exception"""
    pass


def exec_command(cmd, timeout=30):
    """Execute the command

    :param cmd: the linux command that need to be executed
    :param timeout: execution timeout
    :return: Result of execution
    """
    try:
        p = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
        beginning = time.time()
        while True:
            if p.poll() is not None:
                break
            seconds_passed = time.time() - beginning
            if seconds_passed > timeout:
                p.terminate()
                raise TimeoutError(cmd, timeout)
            time.sleep(0.1)
        return_code, stdout = p.wait(), p.stdout.read()
        if return_code == 0:
            return True, stdout
        else:
            return False, stdout
    except TimeoutError as e:
        stdout = 'Exceeded time %s seconds in executing: %s' % (e[1], e[0])
        return False, stdout


class GaleraManager(object):
    def __init__(self, conf):
        self._service = 'mariadb'
        self._parse_config(conf)
        self.result_of_nodes = dict()
        self.first_master = None
        self.safe_to_bootstrap_of_nodes = dict()
        self.last_commit_of_nodes = dict()

    def _parse_config(self, conf):
        config_parser = ConfigParser()
        config_parser.read(conf)
        self._nodes = set(self._parse_nodes(config_parser.get('galera', 'wsrep_cluster_address')))
        self._datadir = config_parser.get('mysqld', 'datadir')
        self._gvwstate_path = os.path.join(self._datadir, 'gvwstate.dat')
        self._grastate_path = os.path.join(self._datadir, 'grastate.dat')

    @staticmethod
    def _parse_nodes(cluster_address):
        if cluster_address.startswith("'") or cluster_address.startswith('"'):
            cluster_address = cluster_address[1:-1]
        cluster_address = cluster_address.split('//')[1]
        return cluster_address.split(',')

    @staticmethod
    def ssh_exec_command(node, str_cmd, timeout=30):
        """Execute the command on the node

        :param node: the ip address or domain name of the node
        :param str_cmd: the linux command that need to be executed
        :param timeout: execution timeout
        :return: result of execution
        """
        return exec_command('ssh %s "%s" ' % (node, str_cmd), timeout)

    def _check_gvwstate(self, node):
        """Check the gvwstate.dat file and delete it if it is empty.

        :param node: the ip address or domain name of the node
        :return: result of execution
        """
        logger.info("check_gvwstate on node[%s]", node)
        str_cmd = "cat %s" % self._gvwstate_path
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res:
            if not stdout:
                str_cmd = "rm -f %s" % self._gvwstate_path
                res, stdout = self.ssh_exec_command(node, str_cmd)
                logger.info(
                    "Empty %s detected on node[%s], removing it to prevent PC recovery failure at next restart ",
                    (self._gvwstate_path, node))
                if res:
                    logger.info("Remove %s successful on node[%s]", self._gvwstate_path, node)
                else:
                    logger.info("Remove %s failed on node[%s] - %s", self._gvwstate_path, node, stdout)
                return res
            else:
                logger.info(
                    "Not empty %s detected on node[%s], removing it to prevent PC recovery failure at next restart ",
                    (self._gvwstate_path, node))
                return True
        else:
            logger.warning("check_gvwstate failed on node[%s] - %s", node, stdout)
            return False

    def get_seqno(self, node):
        logger.info("get_seqno on node[%s]", node)
        str_cmd = r"cat %s | sed -n 's/^seqno.\s*\(.*\)\s*$/\1/p'" % self._grastate_path
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res:
            try:
                return int(stdout)
            except ValueError:
                logger.warning("convert seq_no failed on node[%s]", node)
                return -1
        else:
            logger.warning("get_seqno failed on node[%s] - %s", node, stdout)
            return -1

    def get_recovered_position(self, node):
        """Start with "mysqld_safe –-wsrep_recover" and get the gtid

        :param node: the ip address or domain name of the node
        :return: gtid
        """
        logger.info("galera_recovery on node[%s]", node)
        str_cmd = "galera_recovery"
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res:
            for line in stdout.split('\n'):
                if '--wsrep_start_position' in line:
                    try:
                        position = int(line.split(':')[-1])
                        return position
                    except ValueError:
                        logger.warning("Parse recovered position failed on node[%s]", node)
                        return -1
            logger.warning("Not found --wsrep_start_position on node[%s]", node)
            return -1
        else:
            logger.warning("Galera_recovery failed on node[%s] - %s", node, stdout)
            return -1

    def get_last_commit(self, node):
        self._check_gvwstate(node)
        last_commit = self.get_seqno(node)
        if last_commit == -1:
            last_commit = self.get_recovered_position(node)
        self.last_commit_of_nodes[node] = last_commit

    def get_last_commit_of_nodes(self, nodes):
        thread_pool = ThreadPoolExecutor(max_workers=(len(nodes) + 1))
        all_task = [thread_pool.submit(self.get_last_commit, node) for node in nodes]
        wait(all_task, return_when=ALL_COMPLETED)

    def get_safe_to_bootstrap(self, node):
        logger.info("get_safe_to_bootstrap on node[%s]", node)
        str_cmd = r"cat %s | sed -n 's/^safe_to_bootstrap:\s*\(.*\)$/\1/p'" % self._grastate_path
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res:
            try:
                self.safe_to_bootstrap_of_nodes[node] = int(stdout)
            except ValueError:
                logger.warning("convert safe_to_bootstrap failed on node[%s]", node)
                self.safe_to_bootstrap_of_nodes[node] = 0
        else:
            logger.warning("get_safe_to_bootstrap failed on node[%s] - %s", node, stdout)
            self.safe_to_bootstrap_of_nodes[node] = 0

    def get_safe_to_bootstrap_of_nodes(self, nodes):
        thread_pool = ThreadPoolExecutor(max_workers=(len(nodes) + 1))
        all_task = [thread_pool.submit(self.get_safe_to_bootstrap, node) for node in nodes]
        wait(all_task, return_when=ALL_COMPLETED)

    def get_first_master(self, nodes):
        self.get_last_commit_of_nodes(nodes)
        self.get_safe_to_bootstrap_of_nodes(nodes)

        best_node = None
        best_commit = 0
        missing_nodes = 0
        for node in nodes:
            if self.safe_to_bootstrap_of_nodes[node]:
                # Galera marked the node as safe to boostrap during shutdown. Let's just
                # pick it as our bootstrap node.
                logger.info("Node %s is marked as safe to bootstrap." % node)
                best_node = node

                # We don't need to wait for the other nodes to report state in this case
                missing_nodes = 0
                break
            last_commit = self.last_commit_of_nodes[node]
            if last_commit == -1:
                logger.info("Waiting on node %s to report database status before Master instances can start." % node)
                missing_nodes = 1
                continue

            # this means -1, or that no commit has occured yet.
            if last_commit == 18446744073709551615L:
                last_commit = 0

            if last_commit > best_commit:
                best_node = node
                best_commit = last_commit

        if missing_nodes == 1:
            logger.info('Could not determine bootstrap node')
            best_node = None

        logger.info('Promoting %s to be our bootstrap node', best_node)
        self.first_master = best_node

    def start_galera_node(self, node):
        """Start galera node

        :param node: the ip address or domain name of the node
        :return: the result of starting galera
        """
        logger.info("galera_new_cluster on node[%s]", node)
        str_cmd = 'galera_new_cluster'
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res and not stdout:
            logger.info("galera_new_cluster successful on node[%s]", node)
            self.result_of_nodes[node] = True
        else:
            logger.error("galera_new_cluster failed on node[%s] - %s", node, stdout)
            self.result_of_nodes[node] = False

    def start_mariadb_node(self, node):
        logger.info("start mariadb on node[%s]", node)
        str_cmd = 'systemctl start %s' % self._service
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res and not stdout:
            logger.info("start mariadb  successful on node[%s]", node)
            self.result_of_nodes[node] = True
        else:
            logger.error("start mariadb  failed on node[%s] - %s", node, stdout)
            self.result_of_nodes[node] = False

    def start_mariadb_nodes(self, nodes):
        thread_pool = ThreadPoolExecutor(max_workers=(len(nodes) + 1))
        all_task = [thread_pool.submit(self.start_mariadb_node, node) for node in nodes]
        wait(all_task, return_when=ALL_COMPLETED)

    def start_galera(self, first_master=None):
        """Start galera cluster

        :return: the result of starting galera cluster
        """
        if len(self._nodes) > 1:
            # get first master
            if first_master:
                self.first_master = first_master
            else:
                self.get_first_master(self._nodes)
            if self.first_master is None:
                return

            # start galera node
            self.start_galera_node(self.first_master)
            # start normal node
            normal_nodes = self._nodes - set([self.first_master])
            self.start_mariadb_nodes(normal_nodes)
        else:
            # single node does not need to start galera cluster, only need to start mariadb
            self.start_mariadb_node('127.0.0.1')

    def stop_mariadb_node(self, node):
        logger.info("stop mariadb on node[%s]", node)
        str_cmd = 'systemctl stop %s' % self._service
        res, stdout = self.ssh_exec_command(node, str_cmd)
        if res and not stdout:
            logger.info("stop mariadb  successful on node[%s]", node)
            self.result_of_nodes[node] = True
        else:
            logger.error("stop mariadb  failed on node[%s] - %s", node, stdout)
            self.result_of_nodes[node] = False

    def stop_mariadb_nodes(self, nodes):
        thread_pool = ThreadPoolExecutor(max_workers=(len(nodes) + 1))
        all_task = [thread_pool.submit(self.stop_mariadb_node, node) for node in nodes]
        wait(all_task, return_when=ALL_COMPLETED)

    def stop_galera(self):
        """stop galera cluster

        :return: the result of stoping galera cluster
        """
        self.stop_mariadb_nodes(self._nodes)


def _start_galera(args):
    print 'start galera cluster'
    galera_manager = GaleraManager(args.conf)
    galera_manager.start_galera(args.node)
    if galera_manager.first_master is None:
        last_commit_list = ['%s:%s' % (node, last_commit) for node, last_commit in
                            galera_manager.last_commit_of_nodes.items()]
        safe_to_bootstrap_of_nodes_list = ['%s:%s' % (node, safe_to_bootstrap) for node, safe_to_bootstrap in
                                           galera_manager.safe_to_bootstrap_of_nodes.items()]
        print 'Could not determine bootstrap node'
        print "The node's last_commit is as follows:\n%s" % '\n'.join(last_commit_list)
        print "The node's safe_to_bootstrap is as follows:\n%s" % '\n'.join(safe_to_bootstrap_of_nodes_list)
        print 'After checking the cluster status you can manually specify the first node to start'
        return
    else:
        successed_nodes = [node for node, res in galera_manager.result_of_nodes.items() if res]
        failed_nodes = [node for node, res in galera_manager.result_of_nodes.items() if not res]
        if failed_nodes:
            print "The failed  nodes are as follows:\n%s" % '\n'.join(failed_nodes)
        if successed_nodes:
            print "The successful nodes are as follows:\n%s" % '\n'.join(successed_nodes)


def _stop_galera(args):
    print 'stop galera cluster'
    galera_manager = GaleraManager(args.conf)
    galera_manager.stop_galera()
    successed_nodes = [node for node, res in galera_manager.result_of_nodes.items() if res]
    failed_nodes = [node for node, res in galera_manager.result_of_nodes.items() if not res]
    if failed_nodes:
        print "The failed  nodes are as follows:\n%s" % '\n'.join(failed_nodes)
    if successed_nodes:
        print "The successful nodes are as follows:\n%s" % '\n'.join(successed_nodes)


def main():
    import argparse

    # create the top-level parser
    parser = argparse.ArgumentParser()

    # create sub parser
    subparsers = parser.add_subparsers()

    parser_start = subparsers.add_parser('start', help='start galera cluster')
    parser_start.add_argument('--node', help='first bootstrap node', default='', type=str, required=False)
    parser_start.add_argument('--conf', help='galera cluster config', default=CONF, type=str, required=False)
    parser_start.set_defaults(func=_start_galera)

    parser_stop = subparsers.add_parser('stop', help='stop galera cluster')
    parser_stop.add_argument('--conf', help='galera cluster config', default=CONF, type=str, required=False)
    parser_stop.set_defaults(func=_stop_galera)

    # parse args
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
