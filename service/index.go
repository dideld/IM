package service

import (
	//"html/template"

	"strconv"
	"webapp/models"
    //"fmt"
	"github.com/gin-gonic/gin"
)

// GetIndex
// @Tags 首页
// @Summary 首页
// @Produce json
// @Success 200 {object} map[string]interface{}  // 修改返回类型
// @Router /index [get]
func GetIndex(c *gin.Context) {
    // ind,err:=template.ParseFiles("index.html")
    // if err!=nil{
    //     panic(err)
    // }
    // ind.Execute(c.Writer,"index")
    c.HTML(200, "login.html", nil)

}
func ToRegister(c *gin.Context){
    // ind,err:=template.ParseFiles("views/user/register.html")
    // if err!=nil{
    //     panic(err)
    // }
    // ind.Execute(c.Writer,"register")
     c.HTML(200, "register.html", "register")
}

func ToChat(c *gin.Context){
//     ind,err:=template.ParseFiles("views/chat/index.html",
//         "views/chat/head.html",
//         "views/chat/foot.html",
//         "views/chat/tabmenu.html",
//         "views/chat/concat.html",
//         "views/chat/group.html",
//         "views/chat/profile.html",
//         "views/chat/main.html",

// )
//     if err!=nil{
//         panic(err)
//     }
//     userId,_:=strconv.Atoi(c.Query("userId"))
//     token:=c.Query("token")
//     user:=models.UserBasic{}
//     user.ID=uint(userId)
//     user.Identity=token
//     fmt.Println("Tochat__________",user)
//     ind.Execute(c.Writer,user)
     userId, _ := strconv.Atoi(c.Query("userId"))
    token := c.Query("token")

    user := models.UserBasic{}
    user.ID = uint(userId)
    user.Identity = token


    c.HTML(200, "chat/index.html", user)
}
func Chat(c *gin.Context ){
    models.Chat(c.Writer,c.Request)
}