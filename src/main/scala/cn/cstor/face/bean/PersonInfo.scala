package cn.cstor.face.bean

/**
  * Created by fengwei on 17/1/19.
  */
class PersonInfo(val id: String, val name: String, val code: String, val sex: String
                 , val img_addr: String, val birthday: String, val nation: String = ""
                 , val mark: String = "")
  extends Serializable {

  /**
    * "id"
    * , "name"
    * , "code"
    * , "sex"
    * , "img_addr"
    * , "nation"
    * , "mark"
    * , "birthday"
    */

  override def toString: String = s"PersonInfo(id=$id,name=$name,code=$code,sex=$sex," +
    s"img_addr=$img_addr, birthday=$birthday, nation=$nation, mark=$mark)"

}
