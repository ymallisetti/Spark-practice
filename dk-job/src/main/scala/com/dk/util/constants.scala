package com.dk.util

object Constants {
  
  val applicationConfgFileName = "dk-application.conf"
  val sectionSchemaVarName = "configurations.sectionSchemas"
  
  //application-conf section name
  val dq="configurations.dataQuality"
  //application-conf section name ends
  //---------------------------------------------------
  
  //application-conf section variable name
  val dq_table_name="ccda_rules_config.csv"
  val dqRulesFilter="dq.orgcode.active.filter"
  val DQRuleLookUpCatQuery = "dq.rule.cat.filter.condition"
  //application-conf section variable name
  //---------------------------------------------------
  
  val HdfsBaseDir="/dharmik/dkjob"
  val RulesFileLocation=HdfsBaseDir.concat("/config/dk_rules.csv")
  val DkJobHistoryLocation = HdfsBaseDir.concat("/tables/jh")
  val DkExceptionsLocation = HdfsBaseDir.concat("/tables/exceptions")
}