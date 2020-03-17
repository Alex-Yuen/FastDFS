<?php
/**
 * 饭粒科技
 * Date: 2020/3/17 * Time: 19:18
 * Author: AnQin <an-qin@qq.com>
 * Copyright © 2020. Hangzhou FanLi Technology Co., Ltd All rights reserved.
 */

namespace an\fdfs;

use FastDFS as fdfs;

class FastDFS extends fdfs {
    public array $storage = [];
    private array $tracker = [];
    private array $config = [
        'group_name' => 'group1',
        'domain'     => '',
        'host'       => '127.0.0.1',
        'port'       => '22122',
    ];

    public function __construct(array $config, int $config_index = 0, bool $bMultiThread = false) {
        $this->config = array_merge($this->config, $config);
        $this->tracker = $this->connect_server($this->config['host'], $this->config['port']);
        $this->storage = $this->tracker_query_storage_store($this->config['group_name'], $this->tracker);
        parent::__construct($config_index, $bMultiThread);
    }

    public function getGroups(): ?array {
        $groups = $this->tracker_list_groups(null, $this->tracker);
        return $groups === false ? null : $groups;
    }

    /**
     *
     * @param string      $file 要上传文件的路径
     * @param string|null $ext_name
     * @param string|null $group_name
     * @param array       $meta 数组，文件附带的元元素。如array('width'=>1024,'height'=>768)
     * @return array  成功返回数组 array('group_name'=>'xx','filename'=>'yy') 失败返回 ['error'=>'错误信息']
     */
    public function upload(string $file, ?string $ext_name = null, ?string $group_name = null, array $meta = []): array {
        if (file_exists($file) === false) return ['error' => 'No such file'];
        $ext_name = $ext_name ?? strtolower(pathinfo($file, PATHINFO_EXTENSION));
        $resp = $this->storage_upload_by_filename($file, $ext_name, $meta, $this->getGroupName($group_name), $this->tracker, $this->storage);
        return $resp === false ? ['error' => $this->get_last_error_info()] : $resp;
    }

    private function getGroupName(?string $group_name = null): string {
        return $group_name ?? $this->config['group_name'];
    }

    /**
     *
     * @param string      $file 续传的文件的路径
     * @param string      $filename 被续传的文件名
     * @param string|null $group_name
     * @return bool
     */
    public function modify(string $file, string $filename, ?string $group_name = null): bool {
        if (file_exists($file) === false) return false;
        return $this->storage_modify_by_filename($file, 0, $this->getGroupName($group_name), $filename, $this->tracker, $this->storage);
    }

    /**
     *
     * @param string      $buff 要上传的数据
     * @param string|null $ext_name
     * @param string|null $group_name
     * @param array       $meta 数组，文件附带的元元素。如array('width'=>1024,'height'=>768)
     * @return array  成功返回数组 array('group_name'=>'xx','filename'=>'yy') 失败返回 ['error'=>'错误信息']
     */
    public function upload_buff(string $buff, ?string $ext_name = null, ?string $group_name = null, array $meta = []): array {
        if (empty($buff) === true) return ['error' => 'Buff empty'];
        $resp = $this->storage_upload_by_filebuff($buff, $ext_name, $meta, $this->getGroupName($group_name), $this->tracker, $this->storage);
        return $resp === false ? ['error' => $this->get_last_error_info()] : $resp;
    }

    /**
     *
     * @param string      $buff 续传的数据
     * @param string      $filename 被续传的文件名
     * @param string|null $group_name
     * @return bool
     */
    public function modify_buff(string $buff, string $filename, ?string $group_name = null): bool {
        if (empty($buff) === true) return false;
        return $this->storage_modify_by_filebuff($buff, 1, $this->getGroupName($group_name), $filename, $this->tracker, $this->storage);
    }

    /**
     * 文件读取
     * @param string $filename 组名
     * @param string $group_name 文件名
     * @param int    $file_offset 文件读取的开始位置
     * @param int    $download_bytes 读取的大小
     * @return string|null  成功返回文件内容；失败返回false
     */
    public function download(string $filename, ?string $group_name = null, int $file_offset = 0, int $download_bytes = 0): ?string {
        $content = $this->storage_download_file_to_buff($this->getGroupName($group_name), $filename, $file_offset, $download_bytes, $this->tracker, $this->storage);
        return $content === false ? null : $content;
    }

    /**
     * 创建下载链接
     * @param string      $filename
     * @param string|null $group_name
     * @return string|null
     */
    public function downloadUrl(string $filename, ?string $group_name = null): ?string {
        if ($this->has($filename)) return $this->config['domain'] . '/' . $this->getGroupName($group_name) . '/' . $filename; else return null;
    }

    /**
     * 文件是否存在
     * @param string      $filename
     * @param string|null $group_name
     * @return bool
     */
    public function has(string $filename, ?string $group_name = null): bool {
        return $this->storage_file_exist($this->getGroupName($group_name), $filename);
    }

    /**
     * 文件是否存在
     * @param string      $filename
     * @param string|null $group_name
     * @return array|null
     */
    public function getMetadata(string $filename, ?string $group_name = null): ?array {
        $metadata = $this->storage_get_metadata($this->getGroupName($group_name), $filename, $this->tracker, $this->storage);
        return $metadata !== false ? $metadata : null;
    }

    public function get_file_info($filename, $group_name = null): array {
        $fileInfo = parent::get_file_info($this->getGroupName($group_name), $filename);
        return $fileInfo === false ? [] : $fileInfo;
    }

    public function getLastErrorInfo(): string { return $this->get_last_error_info(); }

    /**
     * 文件删除
     * @param string      $filename 组名
     * @param string|null $group_name 文件名
     * @return bool
     */
    public function delete(string $filename, ?string $group_name = null): bool {
        return fastdfs_storage_delete_file($this->getGroupName($group_name), $filename);
    }

    public function __destruct() {
        $this->close();
    }
}