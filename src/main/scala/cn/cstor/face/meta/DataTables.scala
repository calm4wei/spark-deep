package cn.cstor.face.meta

/**
  * Created on 2017/1/6
  *
  * @author feng.wei
  */
case class DataTables(namespace: String) extends Serializable {

    object FaceInfo extends Serializable {
        val name = s"$namespace:id_code"
        // // 头像地址，姓名，性别，名族，出生年月，身份证号，详细信息（例如禁止出境等备注信息）
        val detail = List(
            "id"
            , "name"
            , "code"
            , "sex"
            , "img_addr"
            , "nation"
            , "mark"
            , "birthday"
        )
    }

}
