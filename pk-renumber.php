<?php

/*
# Описание

Решение проблемы Duplicate entry '2147483647' for key 'PRIMARY' в Битриксе. [Описание сути проблемы](http://pushorigin.ru/bitrix/2147483647-key-primary)

Скрипт создает две временные таблицы, перенумерует ID "как надо", и заменяет существующие данные:

1. переносим b_iblock_element_prop_m45 во временную таблицу, сохраняя "прежнее значение" ID и создаем новые
2. исправляем во временной таблице b_iblock_element_prop_s45, подменяя ID на "новый"
3. заменяем в исходных таблицах

*/

// только из CLI
if(php_sapi_name() !== 'cli') die('Access denied');

// скрипт находится во втором по вложенности каталоге от корня Bitrix
$_SERVER["DOCUMENT_ROOT"] = dirname(dirname(__DIR__));

define("SITE_ID", "ru");
define("NO_KEEP_STATISTIC", true);
define("NOT_CHECK_PERMISSIONS", true);
define("BX_BUFFER_USED", true); // Отключаем буферизацию вывода
require($_SERVER['DOCUMENT_ROOT'].'/bitrix/modules/main/include/prolog_before.php');

class PKRenumber {

  protected $db, $tables, $refRow, $oldNewMap = array();
  public function __construct() {
    global $DB;
    $this->db = $DB;
  }

  // какие таблицы участвуют?
  public function tables($tables) {
    $this->tables = $tables;
    return $this;
  }

  // какое следует обновлять?
  public function refRow($refRow) {
    $this->refRow = $refRow;
    return $this;
  }

  public function run() {
    $this->dropTmpTables();
    $this->createTmpTables();
    $this->lockTables();
    $this->insertTmpTables();
    $this->renumID();
    $this->getIDMap();
    $this->updateRef();
    $this->updateTables();
    $this->unlockTables();
  }

  // SQL: LOCK TABLES `b_iblock_element_prop_m45` WRITE ...
  protected function lockTables() {
    $query = sprintf("LOCK TABLES %s", implode(', ', array_map(function($el){
      return '`' . $el . '` WRITE, `' . $el . '_tmp` WRITE';
    }, $this->tables)));
    $this->db->query($query);
  }

  protected function unlockTables() {
    $this->db->query("UNLOCK TABLES");
  }

  protected function dropTmpTables() {
    foreach($this->tables as $k => $table) {
      $query = sprintf("DROP TABLE IF EXISTS `%s_tmp`", $table, $table);
      $this->db->query($query);
    }
  }

  protected function createTmpTables() {
    foreach($this->tables as $k => $table) {
      // $query = sprintf("CREATE TEMPORARY TABLE `%s_tmp` LIKE `%s`", $table, $table);
      $query = sprintf("CREATE TABLE `%s_tmp` LIKE `%s`", $table, $table);
      $this->db->query($query);
    }
  }

  protected function insertTmpTables() {
    $query = sprintf("INSERT INTO `%s_tmp` SELECT * FROM `%s`", $this->tables['pk_renum'], $this->tables['pk_renum']);
    $this->db->query($query);

    $query = sprintf("INSERT INTO `%s_tmp` SELECT * FROM `%s`", $this->tables['pk_ref'], $this->tables['pk_ref']);
    $this->db->query($query);
  }

  protected function renumID() {
    $query = sprintf("ALTER TABLE `%s_tmp` ADD _OLD_ID INT(11) UNSIGNED NOT NULL DEFAULT '0'", $this->tables['pk_renum']);
    $this->db->query($query);

    // old ID
    $query = sprintf("UPDATE `%s_tmp` SET _OLD_ID = ID", $this->tables['pk_renum']);
    $this->db->query($query);

    // new ID
    $query = sprintf("SET @t=0");
    $this->db->query($query);
    $query = sprintf("UPDATE `%s_tmp` SET `ID` = (@t := @t + 1)", $this->tables['pk_renum']);
    $this->db->query($query);
  }

  // карта старый ID => новый ID
  protected function getIDMap() {
    $query = sprintf("SELECT `ID`, `_OLD_ID` FROM `%s_tmp`", $this->tables['pk_renum']);
    $list = $this->db->query($query);
    while($item = $list->fetch()) {
      $this->oldNewMap[$item['_OLD_ID']] = $item['ID'];
    }
  }

  protected function updateRef() {
    $query = sprintf("SELECT `IBLOCK_ELEMENT_ID`, `%s` FROM `%s_tmp`", $this->refRow, $this->tables['pk_ref']);
    $list = $this->db->query($query);
    while($item = $list->fetch()) {
      $data = unserialize($item[$this->refRow]);
      // $data['ID'] - это необходимо перенумеровать
      // var_dump(unserialize($item[$this->refRow])['ID']); // до
      foreach($data['ID'] as $k => $val) {
        try {
          if(!isset($this->oldNewMap[$val])) throw new Exception("Wrong PK: " . $val);
          $data['ID'][$k] = $this->oldNewMap[$val];
        } catch (Exception $e) {
          $this->error($e->getMessage());
          $data['ID'][$k] = 0;
        }
      }
      $item[$this->refRow] = serialize($data);
      // var_dump(unserialize($item[$this->refRow])['ID']); // после

      $query = sprintf("UPDATE `%s_tmp` SET `%s` = '%s' WHERE `IBLOCK_ELEMENT_ID` = %d", $this->tables['pk_ref'], $this->refRow, $item[$this->refRow], $item['IBLOCK_ELEMENT_ID']);
      $this->db->query($query);
    }
  }

  protected function updateTables() {
    $query = sprintf("ALTER TABLE `%s_tmp` DROP _OLD_ID", $this->tables['pk_renum']);
    $this->db->query($query);

    $query = sprintf("TRUNCATE `%s`", $this->tables['pk_renum']);
    $this->db->query($query);

    $query = sprintf("INSERT INTO `%s` SELECT * FROM `%s_tmp`", $this->tables['pk_renum'], $this->tables['pk_renum']);
    $this->db->query($query);

    $query = sprintf("TRUNCATE `%s`", $this->tables['pk_ref']);
    $this->db->query($query);

    $query = sprintf("INSERT INTO `%s` SELECT * FROM `%s_tmp`", $this->tables['pk_ref'], $this->tables['pk_ref']);
    $this->db->query($query);

    $query = sprintf("DROP TABLE `%s_tmp`", $this->tables['pk_renum']);
    $this->db->query($query);

    $query = sprintf("DROP TABLE `%s_tmp`", $this->tables['pk_ref']);
    $this->db->query($query);
  }

  protected function error($m) {
    fwrite(STDERR, $m . PHP_EOL);
  }

}

$PKRenumber = new PKRenumber();
$PKRenumber
  ->tables(
    array(
      'pk_renum' => 'b_iblock_element_prop_m45',
      'pk_ref' => 'b_iblock_element_prop_s45'
    ))
  ->refRow('PROPERTY_157')
  ->run();
